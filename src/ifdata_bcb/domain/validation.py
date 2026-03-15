import re
from typing import Any

from pydantic import BaseModel, field_validator

from ifdata_bcb.domain.exceptions import (
    InvalidDateFormatError,
    InvalidIdentifierError,
)
from ifdata_bcb.domain.types import AccountInput, DateInput, InstitutionInput


class NormalizedDates(BaseModel):
    """Normaliza DateInput -> list[int] no formato YYYYMM."""

    values: list[int]

    @field_validator("values", mode="before")
    @classmethod
    def normalize(cls, v: DateInput) -> list[int]:
        if not isinstance(v, list):
            v = [v]

        result = []
        for d in v:
            if isinstance(d, int):
                result.append(d)
            elif isinstance(d, str):
                clean = d.replace("-", "").replace("/", "")[:6]
                try:
                    result.append(int(clean))
                except ValueError:
                    raise InvalidDateFormatError(str(d))
            else:
                raise InvalidDateFormatError(str(d))
        return result


class ValidatedCnpj8(BaseModel):
    """Valida CNPJ de 8 digitos."""

    value: str

    @field_validator("value", mode="before")
    @classmethod
    def validate_cnpj(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise InvalidIdentifierError(str(v))
        v = v.strip()
        if not re.fullmatch(r"\d{8}", v):
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
            if not re.fullmatch(r"\d{8}", item):
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
        return list(v)


__all__ = [
    "NormalizedDates",
    "ValidatedCnpj8",
    "InstitutionList",
    "AccountList",
]
