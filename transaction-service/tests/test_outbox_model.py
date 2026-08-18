import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import inspect

from app.database import Base
from app.models import OutboxEvent


def test_outbox_events_table_exists(db_session):
    inspector = inspect(db_session.get_bind())
    tables = inspector.get_table_names()
    assert "outbox_events" in tables


def test_outbox_events_columns(db_session):
    inspector = inspect(db_session.get_bind())
    columns = {col["name"] for col in inspector.get_columns("outbox_events")}

    expected = {
        "id", "event_id", "event_type", "aggregate_id",
        "payload", "status", "created_at", "published_at", "attempts",
        "next_retry_at",
    }
    assert expected == columns


def test_outbox_event_created_with_defaults(db_session):
    event = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"test": true}',
    )
    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    assert event.status == "pending"
    assert event.attempts == 0
    assert event.published_at is None
    assert event.created_at is not None
    assert isinstance(event.created_at, datetime)


def test_outbox_event_persist_and_retrieve(db_session):
    event_id = uuid.uuid4()
    agg_id = uuid.uuid4()

    event = OutboxEvent(
        event_id=event_id,
        event_type="transactions.created",
        aggregate_id=agg_id,
        payload='{"transactionExternalId": "abc"}',
    )
    db_session.add(event)
    db_session.commit()

    retrieved = db_session.query(OutboxEvent).filter(
        OutboxEvent.event_id == event_id
    ).first()

    assert retrieved is not None
    assert retrieved.event_type == "transactions.created"
    assert retrieved.aggregate_id == agg_id
    assert retrieved.status == "pending"
    assert retrieved.attempts == 0


def test_outbox_event_update_status(db_session):
    event = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"test": 1}',
    )
    db_session.add(event)
    db_session.commit()

    event.status = "published"
    event.published_at = datetime.now(timezone.utc)
    db_session.commit()
    db_session.refresh(event)

    assert event.status == "published"
    assert event.published_at is not None


def test_outbox_event_update_attempts(db_session):
    event = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"test": 2}',
    )
    db_session.add(event)
    db_session.commit()

    event.attempts = 1
    db_session.commit()
    db_session.refresh(event)
    assert event.attempts == 1

    event.attempts = 2
    db_session.commit()
    db_session.refresh(event)
    assert event.attempts == 2


def test_outbox_event_to_dlq(db_session):
    event = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"test": 3}',
        attempts=3,
    )
    db_session.add(event)
    db_session.commit()

    event.status = "dlq"
    db_session.commit()
    db_session.refresh(event)

    assert event.status == "dlq"
    assert event.attempts == 3
    assert event.published_at is None


def test_outbox_event_query_pending(db_session):
    e1 = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"q": 1}',
        status="pending",
    )
    e2 = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"q": 2}',
        status="published",
    )
    db_session.add_all([e1, e2])
    db_session.commit()

    pending = db_session.query(OutboxEvent).filter(
        OutboxEvent.status == "pending"
    ).all()

    pending_ids = [e.event_id for e in pending]
    assert e1.event_id in pending_ids
    assert e2.event_id not in pending_ids


def test_outbox_event_query_by_attempts_threshold(db_session):
    e1 = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"a": 1}',
        attempts=2,
        status="pending",
    )
    e2 = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"a": 2}',
        attempts=3,
        status="pending",
    )
    db_session.add_all([e1, e2])
    db_session.commit()

    ready = db_session.query(OutboxEvent).filter(
        OutboxEvent.status == "pending",
        OutboxEvent.attempts < 3,
    ).all()

    ready_ids = [e.event_id for e in ready]
    assert e1.event_id in ready_ids
    assert e2.event_id not in ready_ids


def test_outbox_event_unique_event_id(db_session):
    shared_id = uuid.uuid4()

    e1 = OutboxEvent(
        event_id=shared_id,
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"u": 1}',
    )
    db_session.add(e1)
    db_session.commit()

    e2 = OutboxEvent(
        event_id=shared_id,
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"u": 2}',
    )
    db_session.add(e2)

    with pytest.raises(Exception):
        db_session.commit()


def test_outbox_event_aggregate_id_indexed(db_session):
    inspector = inspect(db_session.get_bind())
    indexes = inspector.get_indexes("outbox_events")
    indexed_cols = set()
    for idx in indexes:
        indexed_cols.update(idx["column_names"])

    assert "aggregate_id" in indexed_cols
