import json
import uuid

from app.models import OutboxEvent, Transaction


def test_create_transaction_valid(client):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": 120,
    })
    assert response.status_code == 201
    data = response.json()
    assert "transactionExternalId" in data
    uuid.UUID(data["transactionExternalId"])


def test_create_transaction_starts_pending(client, db_session):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": 500,
    })
    assert response.status_code == 201
    tx_id = response.json()["transactionExternalId"]

    response = client.get(f"/transactions/{tx_id}")
    assert response.status_code == 200
    assert response.json()["transactionStatus"]["name"] == "pending"


def test_get_transaction_exists(client):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": 100,
    })
    tx_id = response.json()["transactionExternalId"]

    response = client.get(f"/transactions/{tx_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["transactionExternalId"] == tx_id
    assert data["transactionType"]["name"] == "transfer"
    assert data["transactionStatus"]["name"] == "pending"
    assert float(data["value"]) == 100.00


def test_get_transaction_not_found(client):
    fake_id = str(uuid.uuid4())
    response = client.get(f"/transactions/{fake_id}")
    assert response.status_code == 404
    assert response.json()["detail"] == "Transacción no encontrada"


def test_create_transaction_invalid_uuid(client):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": "not-a-uuid",
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": 100,
    })
    assert response.status_code == 422


def test_create_transaction_value_not_positive(client):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": -10,
    })
    assert response.status_code == 422


def test_create_transaction_value_zero(client):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": 0,
    })
    assert response.status_code == 422


def test_create_transaction_same_accounts(client):
    account = str(uuid.uuid4())
    response = client.post("/transactions", json={
        "accountExternalIdDebit": account,
        "accountExternalIdCredit": account,
        "transferTypeId": 1,
        "value": 100,
    })
    assert response.status_code == 422


def test_create_transaction_invalid_transfer_type(client):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 999,
        "value": 100,
    })
    assert response.status_code == 422


def test_get_transaction_no_internal_id(client):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": 100,
    })
    tx_id = response.json()["transactionExternalId"]
    response = client.get(f"/transactions/{tx_id}")
    assert "id" not in response.json()


def test_create_transaction_value_1000(client):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": 1000,
    })
    assert response.status_code == 201


def test_get_transaction_invalid_uuid_format(client):
    response = client.get("/transactions/not-a-valid-uuid")
    assert response.status_code == 422


def test_post_creates_outbox_event(client, db_session):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": 120,
    })
    assert response.status_code == 201
    tx_id = response.json()["transactionExternalId"]

    event = db_session.query(OutboxEvent).filter(
        OutboxEvent.aggregate_id == tx_id
    ).first()
    assert event is not None
    assert event.event_type == "transactions.created"
    assert event.status == "pending"
    assert event.attempts == 0


def test_post_creates_transaction_pending(client, db_session):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 1,
        "value": 250,
    })
    assert response.status_code == 201
    tx_id = response.json()["transactionExternalId"]

    transaction = db_session.query(Transaction).filter(
        Transaction.transaction_external_id == tx_id
    ).first()
    assert transaction is not None
    assert transaction.status == "pending"


def test_post_atomicity_no_partial_state(client, db_session):
    response = client.post("/transactions", json={
        "accountExternalIdDebit": str(uuid.uuid4()),
        "accountExternalIdCredit": str(uuid.uuid4()),
        "transferTypeId": 999,
        "value": 100,
    })
    assert response.status_code == 422

    assert db_session.query(Transaction).count() == 0
    assert db_session.query(OutboxEvent).count() == 0


def test_publish_event_to_kafka():
    from unittest.mock import patch
    from app.kafka_producer import publish_event_to_kafka

    with patch("app.kafka_producer.producer") as mock_producer:
        publish_event_to_kafka("transactions.created", "test-key", '{"test": true}')
        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs[1]["topic"] == "transactions.created"
        assert call_kwargs[1]["key"] == "test-key"
        assert call_kwargs[1]["value"] == '{"test": true}'


def test_publish_event_to_kafka_json_payload():
    from unittest.mock import patch
    from app.kafka_producer import publish_event_to_kafka
    import json

    payload = {"field": "value", "number": 42}
    with patch("app.kafka_producer.producer") as mock_producer:
        publish_event_to_kafka("transactions.created", "my-key", json.dumps(payload))
        call_kwargs = mock_producer.produce.call_args
        assert json.loads(call_kwargs[1]["value"]) == payload
