import pytest

from vector_migration.service import DomainError, IdempotencyConflict, MigrationControlPlane


@pytest.fixture
def plane():
    instance = MigrationControlPlane()
    yield instance
    instance.close()


def test_migration_copies_mirrors_verifies_and_cuts_over(plane):
    plane.upsert_document("a", "first", "doc-a")
    started = plane.start_migration("v2", "start-1")
    assert started["copied_documents"] == 1
    write = plane.upsert_document("b", "during migration", "doc-b")
    assert write["written_to"] == ["index-v1", started["target_index_id"]]
    checked = plane.verify(started["migration_id"])
    assert checked["matches"] is True
    assert checked["source"] == checked["target"]
    result = plane.cutover(started["migration_id"], checked["revision"])
    assert result["active_index_id"] == started["target_index_id"]
    assert plane.state()["active_index_id"] == started["target_index_id"]


def test_write_after_verification_invalidates_ready_state(plane):
    migration = plane.start_migration("v2", "start")
    checked = plane.verify(migration["migration_id"])
    assert checked["status"] == "ready"
    plane.upsert_document("new", "fresh data", "write-new")
    state = plane.state()["migrations"][0]
    assert state["status"] == "building"
    assert state["revision"] == checked["revision"] + 1
    with pytest.raises(DomainError, match="verification"):
        plane.cutover(migration["migration_id"], state["revision"])


def test_manifest_mismatch_blocks_cutover(plane):
    plane.upsert_document("seed", "payload", "seed")
    migration = plane.start_migration("v2", "start")
    plane.db.execute(
        "UPDATE index_documents SET content_hash='corrupt' WHERE index_id=?",
        (migration["target_index_id"],),
    )
    checked = plane.verify(migration["migration_id"])
    assert checked["matches"] is False
    with pytest.raises(DomainError, match="verification"):
        plane.cutover(migration["migration_id"], checked["revision"])


def test_cutover_detects_changes_after_verification(plane):
    plane.upsert_document("seed", "payload", "seed")
    migration = plane.start_migration("v2", "start")
    checked = plane.verify(migration["migration_id"])
    plane.db.execute(
        "UPDATE index_documents SET content_hash='late-corruption' WHERE index_id=?",
        (migration["target_index_id"],),
    )
    with pytest.raises(DomainError, match="changed after verification"):
        plane.cutover(migration["migration_id"], checked["revision"])


def test_rollback_and_fallback_dual_write(plane):
    first = plane.start_migration("v2", "start-v2")
    checked = plane.verify(first["migration_id"])
    cutover = plane.cutover(first["migration_id"], checked["revision"])
    write = plane.upsert_document("after", "kept on both sides", "after-cutover")
    assert set(write["written_to"]) == {cutover["active_index_id"], cutover["fallback_index_id"]}
    current = plane.state()["migrations"][0]
    rolled = plane.rollback(first["migration_id"], current["revision"])
    assert rolled["active_index_id"] == "index-v1"
    assert plane._manifest("index-v1") == plane._manifest(cutover["active_index_id"])
    assert plane.state()["audit_events"][-1]["event_type"] == "rollback_completed"


def test_start_is_idempotent_but_payload_reuse_is_rejected(plane):
    one = plane.start_migration("v2", "same-key")
    assert plane.start_migration("v2", "same-key") == one
    with pytest.raises(IdempotencyConflict):
        plane.start_migration("v3", "same-key")


def test_document_write_idempotency_and_conflict(plane):
    first = plane.upsert_document("d", "payload", "write-key")
    assert plane.upsert_document("d", "payload", "write-key") == first
    with pytest.raises(IdempotencyConflict):
        plane.upsert_document("d", "changed", "write-key")


def test_concurrent_migrations_and_duplicate_versions_are_rejected(plane):
    plane.start_migration("v2", "start")
    with pytest.raises(DomainError, match="already in progress"):
        plane.start_migration("v3", "another")
    with pytest.raises(DomainError, match="already exists"):
        plane.start_migration("v2", "duplicate-version")


def test_optimistic_revision_prevents_stale_cutover(plane):
    migration = plane.start_migration("v2", "start")
    checked = plane.verify(migration["migration_id"])
    with pytest.raises(DomainError, match="stale revision"):
        plane.cutover(migration["migration_id"], checked["revision"] - 1)
