import json
import uuid

from app.models import OutboxEvent, Transaction


def _create_transaction(client, value=120):
    debit = str(uuid.uuid4())
    credit = str(uuid.uuid4())
    response = client.post("/transactions", json={
        "accountExternalIdDebit": debit,
        "accountExternalIdCredit": credit,
        "transferTypeId": 1,
        "value": value,
    })
    return response, debit, credit


def test_post_creates_transaction(client, db_session):
    response, _, _ = _create_transaction(client)
    assert response.status_code == 201

    tx_id = response.json()["transactionExternalId"]
    transaction = db_session.query(Transaction).filter(
        Transaction.transaction_external_id == tx_id
    ).first()
    assert transaction is not None
    assert transaction.status == "pending"


def test_post_creates_outbox_event(client, db_session):
    response, _, _ = _create_transaction(client)
    assert response.status_code == 201

    tx_id = response.json()["transactionExternalId"]
    events = db_session.query(OutboxEvent).filter(
        OutboxEvent.aggregate_id == tx_id
    ).all()
    assert len(events) == 1


def test_outbox_event_initial_status(client, db_session):
    response, _, _ = _create_transaction(client)
    tx_id = response.json()["transactionExternalId"]

    event = db_session.query(OutboxEvent).filter(
        OutboxEvent.aggregate_id == tx_id
    ).first()
    assert event.status == "pending"
    assert event.attempts == 0
    assert event.published_at is None


def test_outbox_event_aggregate_id_matches(client, db_session):
    response, _, _ = _create_transaction(client)
    tx_id = response.json()["transactionExternalId"]

    event = db_session.query(OutboxEvent).filter(
        OutboxEvent.aggregate_id == tx_id
    ).first()
    assert str(event.aggregate_id) == tx_id


def test_outbox_event_type(client, db_session):
    response, _, _ = _create_transaction(client)
    tx_id = response.json()["transactionExternalId"]

    event = db_session.query(OutboxEvent).filter(
        OutboxEvent.aggregate_id == tx_id
    ).first()
    assert event.event_type == "transactions.created"


def test_outbox_event_payload_fields(client, db_session):
    response, debit, credit = _create_transaction(client, value=250)
    tx_id = response.json()["transactionExternalId"]

    event = db_session.query(OutboxEvent).filter(
        OutboxEvent.aggregate_id == tx_id
    ).first()
    payload = json.loads(event.payload)

    assert payload["transactionExternalId"] == tx_id
    assert payload["accountExternalIdDebit"] == debit
    assert payload["accountExternalIdCredit"] == credit
    assert payload["transferTypeId"] == 1
    assert payload["value"] == 250.0


def test_outbox_event_payload_excludes_internal_id(client, db_session):
    response, _, _ = _create_transaction(client)
    tx_id = response.json()["transactionExternalId"]

    event = db_session.query(OutboxEvent).filter(
        OutboxEvent.aggregate_id == tx_id
    ).first()
    payload = json.loads(event.payload)

    assert "id" not in payload
    assert "status" not in payload


def test_transaction_and_outbox_persisted_together(client, db_session):
    response, _, _ = _create_transaction(client)
    assert response.status_code == 201
    tx_id = response.json()["transactionExternalId"]

    transaction = db_session.query(Transaction).filter(
        Transaction.transaction_external_id == tx_id
    ).first()
    event = db_session.query(OutboxEvent).filter(
        OutboxEvent.aggregate_id == tx_id
    ).first()

    assert transaction is not None
    assert event is not None


def test_post_works_without_kafka(client, db_session):
    response, _, _ = _create_transaction(client)
    assert response.status_code == 201

    tx_id = response.json()["transactionExternalId"]
    transaction = db_session.query(Transaction).filter(
        Transaction.transaction_external_id == tx_id
    ).first()
    event = db_session.query(OutboxEvent).filter(
        OutboxEvent.aggregate_id == tx_id
    ).first()
    assert transaction is not None
    assert event is not None


def test_no_partial_state_on_validation_error(client, db_session):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 999,
        "value": 100,
    })
    assert response.status_code == 422

    count = db_session.query(Transaction).count()
    outbox_count = db_session.query(OutboxEvent).count()
    assert count == 0
    assert outbox_count == 0


def test_event_id_is_unique(client, db_session):
    response, _, _ = _create_transaction(client)
    tx_id = response.json()["transactionExternalId"]

    event = db_session.query(OutboxEvent).filter(
        OutboxEvent.aggregate_id == tx_id
    ).first()

    duplicate = OutboxEvent(
        event_id=event.event_id,
        event_type="transactions.created",
        aggregate_id=uuid.uuid4(),
        payload='{"test": true}',
    )
    db_session.add(duplicate)

    from sqlalchemy.exc import IntegrityError
    try:
        db_session.commit()
        assert False, "Expected IntegrityError"
    except IntegrityError:
        db_session.rollback()
