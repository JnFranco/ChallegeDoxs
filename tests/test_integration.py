import time
import uuid

import httpx

BASE_URL = "http://localhost:8000"
TIMEOUT = 30
POLL_INTERVAL = 1


def wait_for_service():
    start = time.time()
    while time.time() - start < TIMEOUT:
        try:
            resp = httpx.get(f"{BASE_URL}/health", timeout=5)
            if resp.status_code == 200:
                return True
        except httpx.ConnectError:
            pass
        time.sleep(1)
    return False


def create_transaction(value):
    resp = httpx.post(
        f"{BASE_URL}/transactions",
        json={
            "accountExternalIdDebit": str(uuid.uuid4()),
            "accountExternalIdCredit": str(uuid.uuid4()),
            "transferTypeId": 1,
            "value": value,
        },
        timeout=10,
    )
    assert resp.status_code == 201, f"Expected 201, got {resp.status_code}: {resp.text}"
    return resp.json()["transactionExternalId"]


def get_transaction(tx_id):
    resp = httpx.get(f"{BASE_URL}/transactions/{tx_id}", timeout=10)
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    return resp.json()


def wait_for_status(tx_id, expected_status, timeout=TIMEOUT):
    start = time.time()
    while time.time() - start < timeout:
        data = get_transaction(tx_id)
        if data["transactionStatus"]["name"] == expected_status:
            return data
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Transaction {tx_id} did not reach {expected_status} within {timeout}s")


def test_full_flow():
    assert wait_for_service(), "Transaction Service not available"

    tx_id = create_transaction(120)

    data = get_transaction(tx_id)
    assert data["transactionType"]["name"] == "transfer"
    assert float(data["value"]) == 120

    data = wait_for_status(tx_id, "approved")
    assert data["transactionStatus"]["name"] == "approved"


def test_value_1000_approved():
    tx_id = create_transaction(1000)
    data = wait_for_status(tx_id, "approved")
    assert data["transactionStatus"]["name"] == "approved"


def test_value_1001_rejected():
    tx_id = create_transaction(1001)
    data = wait_for_status(tx_id, "rejected")
    assert data["transactionStatus"]["name"] == "rejected"


def test_value_1000_01_rejected():
    tx_id = create_transaction(1000.01)
    data = wait_for_status(tx_id, "rejected")
    assert data["transactionStatus"]["name"] == "rejected"


def test_health():
    resp = httpx.get(f"{BASE_URL}/health", timeout=5)
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
