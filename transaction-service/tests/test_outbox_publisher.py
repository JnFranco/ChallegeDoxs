import json
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from app.models import OutboxEvent
from app.outbox_publisher import MAX_ATTEMPTS, _process_event


def _create_event(db_session, status="pending", attempts=0, next_retry_at=None):
    event = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload=json.dumps({
            "transactionExternalId": str(uuid.uuid4()),
            "accountExternalIdDebit": str(uuid.uuid4()),
            "accountExternalIdCredit": str(uuid.uuid4()),
            "transferTypeId": 1,
            "value": 120.00,
        }),
        status=status,
        attempts=attempts,
        next_retry_at=next_retry_at,
    )
    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)
    return event


def _select_pending(db_session):
    return (
        db_session.query(OutboxEvent)
        .filter(
            OutboxEvent.status == "pending",
            OutboxEvent.attempts < MAX_ATTEMPTS,
        )
        .order_by(OutboxEvent.created_at)
        .all()
    )


def test_pending_event_is_published(db_session):
    event = _create_event(db_session)

    with patch("app.outbox_publisher.publish_event_to_kafka") as mock_pub:
        result = _process_event(event, db_session)

    assert result is True
    assert event.status == "published"
    assert event.published_at is not None
    assert isinstance(event.published_at, datetime)
    assert event.attempts == 0
    mock_pub.assert_called_once_with(
        topic="transactions.created",
        key=str(event.aggregate_id),
        payload=event.payload,
    )


def test_published_event_is_not_selected_for_processing(db_session):
    event = _create_event(db_session, status="published")

    pending = _select_pending(db_session)
    pending_ids = [e.event_id for e in pending]
    assert event.event_id not in pending_ids


def test_kafka_failure_increments_attempts(db_session):
    event = _create_event(db_session)

    with patch("app.outbox_publisher.publish_event_to_kafka", side_effect=Exception("Kafka caído")):
        _process_event(event, db_session)

    assert event.attempts == 1


def test_kafka_failure_keeps_event_pending(db_session):
    event = _create_event(db_session)

    with patch("app.outbox_publisher.publish_event_to_kafka", side_effect=Exception("Kafka caído")):
        _process_event(event, db_session)

    assert event.status == "pending"


def test_event_moves_to_dlq_after_max_attempts(db_session):
    event = _create_event(db_session, attempts=MAX_ATTEMPTS - 1)

    with patch("app.outbox_publisher.publish_event_to_kafka", side_effect=Exception("Kafka caído")):
        _process_event(event, db_session)

    assert event.status == "dlq"
    assert event.attempts == MAX_ATTEMPTS


def test_dlq_event_is_not_selected_for_processing(db_session):
    event = _create_event(db_session, status="dlq", attempts=MAX_ATTEMPTS)

    pending = _select_pending(db_session)
    pending_ids = [e.event_id for e in pending]
    assert event.event_id not in pending_ids


def test_payload_is_published_unchanged(db_session):
    payload_data = {
        "transactionExternalId": str(uuid.uuid4()),
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": 100.00,
    }
    event = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload=json.dumps(payload_data),
    )
    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    with patch("app.outbox_publisher.publish_event_to_kafka") as mock_pub:
        _process_event(event, db_session)

    sent_payload = mock_pub.call_args.kwargs["payload"]
    assert sent_payload == json.dumps(payload_data)


def test_external_id_is_used_as_kafka_key(db_session):
    agg_id = uuid.uuid4()
    event = OutboxEvent(
        event_id=uuid.uuid4(),
        event_type="transactions.created",
        aggregate_id=agg_id,
        payload='{"test": true}',
    )
    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    with patch("app.outbox_publisher.publish_event_to_kafka") as mock_pub:
        _process_event(event, db_session)

    key = mock_pub.call_args.kwargs["key"]
    assert key == str(agg_id)


def test_published_at_is_set_only_after_success(db_session):
    event = _create_event(db_session)

    with patch("app.outbox_publisher.publish_event_to_kafka", side_effect=Exception("fallo")):
        _process_event(event, db_session)

    assert event.published_at is None


def test_multiple_pending_events_all_published(db_session):
    e1 = _create_event(db_session)
    e2 = _create_event(db_session)
    e3 = _create_event(db_session, status="published")

    with patch("app.outbox_publisher.publish_event_to_kafka") as mock_pub:
        for e in [e1, e2]:
            _process_event(e, db_session)

    assert mock_pub.call_count == 2
    assert e1.status == "published"
    assert e2.status == "published"
    assert e3.status == "published"


def test_shutdown_stops_worker():
    from app.outbox_publisher import _stop_event

    _stop_event.set()
    assert _stop_event.is_set()
    _stop_event.clear()


def test_next_retry_at_skips_event_in_backoff(db_session):
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    event = _create_event(db_session, next_retry_at=future)

    pending = _select_pending(db_session)
    pending_ids = [e.event_id for e in pending]
    assert event.event_id in pending_ids

    now = datetime.now(timezone.utc)
    assert event.next_retry_at > now

    should_skip = event.next_retry_at > now
    assert should_skip is True


def test_backoff_sets_next_retry_at(db_session):
    event = _create_event(db_session)

    with patch("app.outbox_publisher.publish_event_to_kafka", side_effect=Exception("fallo")):
        _process_event(event, db_session)

    assert event.next_retry_at is not None
    assert event.next_retry_at > datetime.now(timezone.utc)


def test_dlq_does_not_set_next_retry(db_session):
    event = _create_event(db_session, attempts=MAX_ATTEMPTS - 1)

    with patch("app.outbox_publisher.publish_event_to_kafka", side_effect=Exception("fallo")):
        _process_event(event, db_session)

    assert event.status == "dlq"
    assert event.next_retry_at is None


def test_retry_backoff_first_attempt_2s(db_session):
    event = _create_event(db_session)

    with patch("app.outbox_publisher.publish_event_to_kafka", side_effect=Exception("fallo")):
        _process_event(event, db_session)

    assert event.attempts == 1
    assert event.next_retry_at is not None
    expected = timedelta(seconds=2)
    actual = event.next_retry_at - event.created_at
    assert abs(actual.total_seconds() - expected.total_seconds()) < 1
