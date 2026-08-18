from decimal import Decimal


def evaluate_fraud(value: Decimal) -> str:
    if value > Decimal("1000"):
        return "rejected"
    return "approved"
