"""In-process SQLite control plane; hashes stand in for vector payloads."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any


class DomainError(Exception):
    """A request conflicts with the migration state or supplied revision."""


class IdempotencyConflict(DomainError):
    """An idempotency key was reused for a different operation or payload."""


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def fingerprint(value: Any) -> str:
    packed = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(packed.encode("utf-8")).hexdigest()


class MigrationControlPlane:
    """Small transactional model of blue/green index migrations and rollback."""

    def __init__(self, database: str = ":memory:") -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(database, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self._schema()

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
            except Exception:
                self.db.rollback()
                raise
            else:
                self.db.commit()

    def _schema(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS indexes (
              index_id TEXT PRIMARY KEY, version TEXT NOT NULL, status TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS documents (
              document_id TEXT PRIMARY KEY, content_hash TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS index_documents (
              index_id TEXT NOT NULL REFERENCES indexes(index_id),
              document_id TEXT NOT NULL, content_hash TEXT NOT NULL,
              PRIMARY KEY(index_id, document_id)
            );
            CREATE TABLE IF NOT EXISTS migrations (
              migration_id TEXT PRIMARY KEY, source_index_id TEXT NOT NULL,
              target_index_id TEXT NOT NULL, status TEXT NOT NULL, revision INTEGER NOT NULL,
              idempotency_key TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
              verified_manifest TEXT
            );
            CREATE TABLE IF NOT EXISTS idempotency (
              idempotency_key TEXT PRIMARY KEY, operation TEXT NOT NULL,
              payload_hash TEXT NOT NULL, result_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS audit_events (
              event_id INTEGER PRIMARY KEY AUTOINCREMENT, occurred_at TEXT NOT NULL,
              migration_id TEXT, event_type TEXT NOT NULL, details_json TEXT NOT NULL
            );
            """
        )
        row = self.db.execute("SELECT 1 FROM indexes LIMIT 1").fetchone()
        if row is None:
            self.db.execute(
                "INSERT INTO indexes VALUES (?, ?, 'active', ?)",
                ("index-v1", "v1", utc_now()),
            )
            self._event(None, "bootstrap", {"active_index_id": "index-v1"})

    def _event(self, migration_id: str | None, kind: str, details: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO audit_events(occurred_at,migration_id,event_type,details_json) VALUES (?,?,?,?)",
            (utc_now(), migration_id, kind, json.dumps(details, sort_keys=True)),
        )

    def _idempotent_result(
        self, key: str, operation: str, payload_hash: str
    ) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT operation,payload_hash,result_json FROM idempotency WHERE idempotency_key=?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        if row["operation"] != operation or row["payload_hash"] != payload_hash:
            raise IdempotencyConflict("idempotency key already belongs to a different request")
        return json.loads(row["result_json"])

    def _save_idempotent(
        self, key: str, operation: str, payload_hash: str, result: dict[str, Any]
    ) -> None:
        self.db.execute(
            "INSERT INTO idempotency VALUES (?,?,?,?)",
            (key, operation, payload_hash, json.dumps(result, sort_keys=True)),
        )

    def _active_index(self) -> sqlite3.Row:
        row = self.db.execute("SELECT * FROM indexes WHERE status='active'").fetchone()
        if row is None:
            raise DomainError("no active index is configured")
        return row

    def start_migration(self, version: str, idempotency_key: str) -> dict[str, Any]:
        payload = {"version": version}
        payload_hash = fingerprint(payload)
        with self._tx():
            replay = self._idempotent_result(idempotency_key, "start_migration", payload_hash)
            if replay is not None:
                return replay
            if not version.strip():
                raise DomainError("version must not be blank")
            if self.db.execute("SELECT 1 FROM indexes WHERE version=?", (version,)).fetchone():
                raise DomainError("an index with this version already exists")
            existing = self.db.execute(
                "SELECT * FROM migrations WHERE status IN ('building','ready') LIMIT 1"
            ).fetchone()
            if existing:
                raise DomainError("another migration is already in progress")
            source = self._active_index()
            self.db.execute("UPDATE indexes SET status='retired' WHERE status='fallback'")
            target_id = f"index-{uuid.uuid4().hex[:12]}"
            migration_id = f"migration-{uuid.uuid4().hex[:12]}"
            now = utc_now()
            self.db.execute(
                "INSERT INTO indexes VALUES (?,?,?,?)", (target_id, version, "building", now)
            )
            self.db.execute(
                "INSERT INTO migrations VALUES (?,?,?,?,?,?,?,NULL)",
                (migration_id, source["index_id"], target_id, "building", 1, idempotency_key, now),
            )
            self.db.execute(
                "INSERT INTO index_documents(index_id,document_id,content_hash) "
                "SELECT ?,document_id,content_hash FROM index_documents WHERE index_id=?",
                (target_id, source["index_id"]),
            )
            result = {
                "migration_id": migration_id,
                "source_index_id": source["index_id"],
                "target_index_id": target_id,
                "status": "building",
                "revision": 1,
                "copied_documents": self._count(target_id),
            }
            self._save_idempotent(idempotency_key, "start_migration", payload_hash, result)
            self._event(migration_id, "migration_started", result)
            return result

    def _count(self, index_id: str) -> int:
        return int(
            self.db.execute(
                "SELECT COUNT(*) FROM index_documents WHERE index_id=?", (index_id,)
            ).fetchone()[0]
        )

    def upsert_document(
        self, document_id: str, content: str, idempotency_key: str
    ) -> dict[str, Any]:
        payload = {"document_id": document_id, "content_hash": fingerprint(content)}
        payload_hash = fingerprint(payload)
        with self._tx():
            replay = self._idempotent_result(idempotency_key, "upsert_document", payload_hash)
            if replay is not None:
                return replay
            if not document_id.strip():
                raise DomainError("document_id must not be blank")
            content_hash = payload["content_hash"]
            self.db.execute(
                "INSERT INTO documents VALUES (?,?,?) ON CONFLICT(document_id) DO UPDATE SET "
                "content_hash=excluded.content_hash,updated_at=excluded.updated_at",
                (document_id, content_hash, utc_now()),
            )
            # Keep active, building, and rollback fallback generations in sync.
            write_indexes = self.db.execute(
                "SELECT index_id FROM indexes WHERE status IN ('active','building','fallback')"
            ).fetchall()
            for index in write_indexes:
                self.db.execute(
                    "INSERT INTO index_documents VALUES (?,?,?) "
                    "ON CONFLICT(index_id,document_id) DO UPDATE SET content_hash=excluded.content_hash",
                    (index["index_id"], document_id, content_hash),
                )
            migration = self.db.execute(
                "SELECT * FROM migrations WHERE status IN ('building','ready') LIMIT 1"
            ).fetchone()
            if migration:
                revision = migration["revision"] + 1
                self.db.execute(
                    "UPDATE migrations SET status='building',revision=?,verified_manifest=NULL "
                    "WHERE migration_id=?",
                    (revision, migration["migration_id"]),
                )
                self.db.execute(
                    "UPDATE indexes SET status='building' WHERE index_id=?",
                    (migration["target_index_id"],),
                )
            result = {
                "document_id": document_id,
                "content_hash": content_hash,
                "written_to": [row["index_id"] for row in write_indexes],
                "migration_revision": revision if migration else None,
            }
            self._save_idempotent(idempotency_key, "upsert_document", payload_hash, result)
            self._event(
                migration["migration_id"] if migration else None,
                "document_upserted",
                {"document_id": document_id, "written_to": result["written_to"]},
            )
            return result

    def _manifest(self, index_id: str) -> dict[str, Any]:
        rows = self.db.execute(
            "SELECT document_id,content_hash FROM index_documents WHERE index_id=? "
            "ORDER BY document_id",
            (index_id,),
        ).fetchall()
        entries = [(row["document_id"], row["content_hash"]) for row in rows]
        return {"count": len(entries), "digest": fingerprint(entries)}

    def verify(self, migration_id: str) -> dict[str, Any]:
        with self._tx():
            migration = self.db.execute(
                "SELECT * FROM migrations WHERE migration_id=?", (migration_id,)
            ).fetchone()
            if migration is None:
                raise DomainError("migration not found")
            source = self._manifest(migration["source_index_id"])
            target = self._manifest(migration["target_index_id"])
            matches = source == target
            status = "ready" if matches else "building"
            revision = migration["revision"] + 1
            manifest = {"source": source, "target": target, "matches": matches}
            self.db.execute(
                "UPDATE migrations SET status=?,revision=?,verified_manifest=? WHERE migration_id=?",
                (status, revision, json.dumps(manifest, sort_keys=True), migration_id),
            )
            self.db.execute(
                "UPDATE indexes SET status=? WHERE index_id=?",
                ("ready" if matches else "building", migration["target_index_id"]),
            )
            result = {
                "migration_id": migration_id,
                "status": status,
                "revision": revision,
                "matches": matches,
                **manifest,
            }
            self._event(migration_id, "verification_completed", result)
            return result

    def cutover(self, migration_id: str, expected_revision: int) -> dict[str, Any]:
        with self._tx():
            migration = self.db.execute(
                "SELECT * FROM migrations WHERE migration_id=?", (migration_id,)
            ).fetchone()
            if migration is None:
                raise DomainError("migration not found")
            if migration["revision"] != expected_revision:
                raise DomainError(f"stale revision: current revision is {migration['revision']}")
            if migration["status"] != "ready" or not migration["verified_manifest"]:
                raise DomainError("migration must pass verification before cutover")
            manifest = json.loads(migration["verified_manifest"])
            if manifest["source"] != self._manifest(migration["source_index_id"]):
                raise DomainError("source changed after verification; verify again")
            if manifest["target"] != self._manifest(migration["target_index_id"]):
                raise DomainError("target changed after verification; verify again")
            source_id, target_id = migration["source_index_id"], migration["target_index_id"]
            self.db.execute("UPDATE indexes SET status='fallback' WHERE index_id=?", (source_id,))
            self.db.execute("UPDATE indexes SET status='active' WHERE index_id=?", (target_id,))
            self.db.execute(
                "UPDATE migrations SET status='cut_over',revision=revision+1 WHERE migration_id=?",
                (migration_id,),
            )
            result = {
                "migration_id": migration_id,
                "status": "cut_over",
                "active_index_id": target_id,
                "fallback_index_id": source_id,
                "revision": migration["revision"] + 1,
            }
            self._event(migration_id, "cutover_completed", result)
            return result

    def rollback(self, migration_id: str, expected_revision: int) -> dict[str, Any]:
        with self._tx():
            migration = self.db.execute(
                "SELECT * FROM migrations WHERE migration_id=?", (migration_id,)
            ).fetchone()
            if migration is None:
                raise DomainError("migration not found")
            if migration["revision"] != expected_revision:
                raise DomainError(f"stale revision: current revision is {migration['revision']}")
            if migration["status"] != "cut_over":
                raise DomainError("only a completed cutover can be rolled back")
            active = self._active_index()
            if active["index_id"] != migration["target_index_id"]:
                raise DomainError("the migrated index is no longer active")
            self.db.execute(
                "UPDATE indexes SET status='active' WHERE index_id=?",
                (migration["source_index_id"],),
            )
            self.db.execute(
                "UPDATE indexes SET status='fallback' WHERE index_id=?", (active["index_id"],)
            )
            self.db.execute(
                "UPDATE migrations SET status='rolled_back',revision=revision+1 WHERE migration_id=?",
                (migration_id,),
            )
            result = {
                "migration_id": migration_id,
                "status": "rolled_back",
                "active_index_id": migration["source_index_id"],
                "fallback_index_id": active["index_id"],
                "revision": migration["revision"] + 1,
            }
            self._event(migration_id, "rollback_completed", result)
            return result

    def state(self) -> dict[str, Any]:
        with self._lock:
            active = self._active_index()
            indexes = [
                dict(row) | {"document_count": self._count(row["index_id"])}
                for row in self.db.execute("SELECT * FROM indexes ORDER BY created_at,index_id")
            ]
            migrations = [
                dict(row)
                for row in self.db.execute(
                    "SELECT migration_id,source_index_id,target_index_id,status,revision,created_at "
                    "FROM migrations ORDER BY created_at"
                )
            ]
            events = [
                dict(row)
                for row in self.db.execute(
                    "SELECT event_id,occurred_at,migration_id,event_type,details_json "
                    "FROM audit_events ORDER BY event_id"
                )
            ]
            return {
                "active_index_id": active["index_id"],
                "indexes": indexes,
                "migrations": migrations,
                "audit_events": events,
            }
