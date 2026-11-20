"""FastAPI surface for the local migration control-plane demo."""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .service import DomainError, MigrationControlPlane

app = FastAPI(title="Vector Index Migration Control Plane", version="0.1.0")
plane = MigrationControlPlane()


class StartMigration(BaseModel):
    version: str = Field(min_length=1, max_length=80)
    idempotency_key: str = Field(min_length=1, max_length=120)


class DocumentWrite(BaseModel):
    document_id: str = Field(min_length=1, max_length=200)
    content: str = Field(max_length=100_000)
    idempotency_key: str = Field(min_length=1, max_length=120)


class RevisionRequest(BaseModel):
    expected_revision: int = Field(ge=1)


def call_service(operation):
    try:
        return operation()
    except DomainError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/state")
def get_state():
    return plane.state()


@app.post("/documents", status_code=200)
def write_document(request: DocumentWrite):
    return call_service(
        lambda: plane.upsert_document(request.document_id, request.content, request.idempotency_key)
    )


@app.post("/migrations", status_code=201)
def start_migration(request: StartMigration):
    return call_service(lambda: plane.start_migration(request.version, request.idempotency_key))


@app.post("/migrations/{migration_id}/verify")
def verify_migration(migration_id: str):
    return call_service(lambda: plane.verify(migration_id))


@app.post("/migrations/{migration_id}/cutover")
def cutover(migration_id: str, request: RevisionRequest):
    return call_service(lambda: plane.cutover(migration_id, request.expected_revision))


@app.post("/migrations/{migration_id}/rollback")
def rollback(migration_id: str, request: RevisionRequest):
    return call_service(lambda: plane.rollback(migration_id, request.expected_revision))
