# Vector Index Migration Control Plane

A runnable Python backend demo for one operationally risky RAG change: re-building a vector index without losing writes or making an unverified generation active. It models a blue/green migration with an explicit state machine, mirrored writes, deterministic manifest checks, an atomic alias cutover, optimistic revisions, idempotency keys, a rollback generation, and an append-only audit trail.

## Why this is different

The portfolio already includes semantic retrieval, embedding drift monitoring, RAG evaluation, and model rollout gating. This project focuses on the control-plane problem between them: how an index generation is copied, kept current during a rebuild, checked for parity, and switched or rolled back safely.

## Quick start

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
uvicorn vector_migration.api:app --app-dir src --reload
```

Open `http://127.0.0.1:8000/docs`. The in-memory demo starts with active index `index-v1`.

Example sequence:

1. `POST /documents` with `document_id`, `content`, and a unique `idempotency_key`.
2. `POST /migrations` with a new `version` and unique `idempotency_key`; the target begins as a copied shadow generation.
3. Keep sending document writes; the active and building generations receive the same content fingerprint. Writes invalidate a prior READY check.
4. `POST /migrations/{id}/verify`; cutover is allowed only when deterministic source and target manifests match.
5. `POST /migrations/{id}/cutover` with the exact `expected_revision` returned by verification. The source stays available as a write-through fallback; rollback is guarded by the same revision check.

A later generation retires the older fallback. The `GET /state` response exposes index/migration state and the audit events for inspection.

## Design notes

- SQLite transactions use `BEGIN IMMEDIATE`; each state transition and its audit record commit together.
- Document writes are idempotent by key and mirrored to active, building, and rollback-fallback generations.
- Manifest parity is a sorted SHA-256 fingerprint over document IDs and content hashes. Cutover rechecks the verified manifests inside the write transaction.
- State-changing operations reject stale revisions rather than silently applying an old operator decision.
- The tests cover API conflicts, idempotency, mirrored writes, invalidated verification, parity failures, cutover and rollback.

## Scope and limits

This is an educational, single-process SQLite simulation. It stores content hashes, not embeddings; it does not connect to a vector database, calculate retrieval quality, authenticate callers, provide tenant isolation, or claim production availability. The API should not be exposed publicly or used with confidential documents. The parity check proves equality of this demo's stored manifests only; it is not a guarantee of real ANN index equivalence.

## Checks

```bash
ruff check .
pytest -q
```
