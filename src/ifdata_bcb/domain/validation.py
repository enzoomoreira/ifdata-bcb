import re
from typing import Any

from pydantic import BaseModel, field_validator

from ifdata_bcb.domain.exceptions import (
    InvalidIdentifierError,
)
from ifdata_bcb.domain.types import AccountInput, DateInput, InstitutionInput
from ifdata_bcb.utils.date import normalize_date_to_int


class NormalizedDates(BaseModel):
    """Normaliza DateInput -> list[int] no formato YYYYMM."""

    values: list[int]

    @field_validator("values", mode="before")
    @classmethod
    def normalize(cls, v: DateInput) -> list[int]:
        items = v if isinstance(v, list) else [v]
        return [normalize_date_to_int(d) for d in items]


class ValidatedCnpj8(BaseModel):
    """Valida CNPJ de 8 digitos."""

    value: str

    @field_validator("value", mode="before")
    @classmethod
    def validate_cnpj(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise InvalidIdentifierError(str(v))
        v = v.strip()
        if not re.fullmatch(r"[0-9]{8}", v):
            raise InvalidIdentifierError(v)
        return v


class InstitutionList(BaseModel):
    """Normaliza InstitutionInput -> list[str] de CNPJs validados."""

    values: list[str]

    @field_validator("values", mode="before")
    @classmethod
    def normalize_and_validate(cls, v: InstitutionInput) -> list[str]:
        if isinstance(v, str):
            v = [v]

        result = []
        for item in v:
            item = item.strip()
            if not re.fullmatch(r"[0-9]{8}", item):
                raise InvalidIdentifierError(item)
            result.append(item)
        return result


class AccountList(BaseModel):
    """Normaliza AccountInput -> list[str]."""

    values: list[str]

    @field_validator("values", mode="before")
    @classmethod
    def normalize(cls, v: AccountInput) -> list[str]:
        if isinstance(v, str):
            return [v]
        if not hasattr(v, "__iter__"):
            return [str(v)]
        return [str(item) for item in v]


__all__ = [
    "NormalizedDates",
    "ValidatedCnpj8",
    "InstitutionList",
    "AccountList",
]
