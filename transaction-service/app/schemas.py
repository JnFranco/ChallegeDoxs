from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class TransactionCreate(BaseModel):
    accountExternalIdDebit: UUID = Field(
        description="UUID de la cuenta de origen (débito)",
        examples=["2b894fb0-09f1-4d46-a610-0c92a5c4e113"],
    )
    accountExternalIdCredit: UUID = Field(
        description="UUID de la cuenta de destino (crédito). Debe ser distinta a la de origen.",
        examples=["045d5400-e3cf-4e57-9fe4-a9815eeec2c4"],
    )
    transferTypeId: int = Field(
        description="ID del tipo de transferencia (1 = transfer)",
        examples=[1],
    )
    value: Decimal = Field(
        description="Monto de la transacción. Debe ser mayor que 0.",
        examples=[120],
    )

    @field_validator("value")
    @classmethod
    def value_must_be_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("value debe ser mayor que 0")
        return v


class TransactionTypeOut(BaseModel):
    name: str = Field(description="Nombre del tipo de transferencia")


class TransactionStatusOut(BaseModel):
    name: str = Field(description="Estado actual: pending, approved o rejected")


class TransactionCreatedResponse(BaseModel):
    transactionExternalId: UUID = Field(
        description="UUID único de la transacción creada. Usalo para consultarla después.",
    )


class TransactionResponse(BaseModel):
    transactionExternalId: UUID = Field(description="UUID de la transacción")
    transactionType: TransactionTypeOut = Field(description="Tipo de transferencia")
    transactionStatus: TransactionStatusOut = Field(description="Estado actual de la transacción")
    value: Decimal = Field(description="Monto de la transacción")
    createdAt: datetime = Field(description="Fecha y hora de creación (UTC)")
