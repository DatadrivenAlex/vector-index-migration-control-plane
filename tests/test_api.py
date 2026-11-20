from fastapi.testclient import TestClient

from vector_migration.api import app, plane


def test_http_flow_and_conflict_responses():
    # The module app has one in-memory plane; use unique keys for repeatable CI runs.
    client = TestClient(app)
    assert client.get("/health").json() == {"status": "ok"}
    start = client.post(
        "/migrations", json={"version": "api-test-v2", "idempotency_key": "api-start-unique"}
    )
    assert start.status_code == 201
    migration = start.json()
    verify = client.post(f"/migrations/{migration['migration_id']}/verify")
    assert verify.status_code == 200
    cutover = client.post(
        f"/migrations/{migration['migration_id']}/cutover",
        json={"expected_revision": verify.json()["revision"] - 1},
    )
    assert cutover.status_code == 409
    assert "stale revision" in cutover.json()["detail"]
    assert plane.state()["active_index_id"] == "index-v1"
