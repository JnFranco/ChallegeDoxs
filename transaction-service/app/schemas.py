from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class TransactionCreate(BaseModel):
    accountExternalIdDebit: UUID = Field(
        ...,
        description="UUID de la cuenta de origen (débito)",
        json_schema_extra={
            "examples": ["2b894fb0-09f1-4d46-a610-0c92a5c4e113"],
            "title": "Cuenta de Origen (UUID)",
        },
    )
    accountExternalIdCredit: UUID = Field(
        ...,
        description="UUID de la cuenta de destino (crédito). Debe ser distinta a la de origen.",
        json_schema_extra={
            "examples": ["045d5400-e3cf-4e57-9fe4-a9815eeec2c4"],
            "title": "Cuenta de Destino (UUID)",
        },
    )
    transferTypeId: int = Field(
        ...,
        description="ID del tipo de transferencia. Usualmente 1 para transferencia estándar.",
        json_schema_extra={
            "examples": [1],
            "title": "Tipo de Transferencia",
        },
    )
    value: Decimal = Field(
        ...,
        description="Monto de la transacción. Debe ser mayor que 0.",
        json_schema_extra={
            "examples": [120],
            "title": "Monto",
        },
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "accountExternalIdDebit": "2b894fb0-09f1-4d46-a610-0c92a5c4e113",
                    "accountExternalIdCredit": "045d5400-e3cf-4e57-9fe4-a9815eeec2c4",
                    "transferTypeId": 1,
                    "value": 120,
                }
            ]
        }
    }

    @field_validator("value")
    @classmethod
    def value_must_be_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("El monto debe ser mayor que 0")
        return v


class TransactionTypeOut(BaseModel):
    name: str = Field(
        description="Nombre del tipo de transferencia",
        examples=["transfer"],
    )


class TransactionStatusOut(BaseModel):
    name: str = Field(
        description="Estado actual de la transacción",
        examples=["pending"],
        json_schema_extra={
            "title": "Estado",
            "description": "Estado de la transacción: pending (pendiente), approved (aprobada) o rejected (rechazada)",
        },
    )


class TransactionCreatedResponse(BaseModel):
    transactionExternalId: UUID = Field(
        description="UUID único de la transacción creada. Copialo y usalo en GET /transactions/{id} para consultar el estado.",
        json_schema_extra={
            "title": "ID de la Transacción",
        },
    )


class TransactionResponse(BaseModel):
    transactionExternalId: UUID = Field(
        description="UUID de la transacción",
        json_schema_extra={"title": "ID de la Transacción"},
    )
    transactionType: TransactionTypeOut = Field(
        description="Tipo de transferencia",
        json_schema_extra={"title": "Tipo de Transferencia"},
    )
    transactionStatus: TransactionStatusOut = Field(
        description="Estado actual de la transacción",
        json_schema_extra={"title": "Estado"},
    )
    value: Decimal = Field(
        description="Monto de la transacción",
        json_schema_extra={"title": "Monto"},
    )
    createdAt: datetime = Field(
        description="Fecha y hora de creación (UTC)",
        json_schema_extra={"title": "Fecha de Creación"},
    )
