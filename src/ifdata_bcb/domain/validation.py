import re
from typing import Any

from pydantic import BaseModel, field_validator

from ifdata_bcb.domain.exceptions import InvalidIdentifierError
from ifdata_bcb.domain.types import AccountInput, DateInput, InstitutionInput
from ifdata_bcb.utils.cnpj import is_valid_cnpj14, only_digits
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
    """Valida e normaliza CNPJ para a base de 8 digitos.

    Aceita o que o usuario tipicamente cola do sistema dele: a base de 8
    digitos com ou sem formatacao ('60872504', '60.872.504', '6087 2504') e o
    CNPJ completo de 14 digitos, tambem com ou sem formatacao.

    No caso de 14 digitos os digitos verificadores sao conferidos antes do
    truncamento -- '99999999999999' nao passa. O DV nao se aplica a base de 8
    isolada, entao la nao ha o que checar.

    Nao ha zfill: um valor com menos de 8 digitos e erro de digitacao do
    usuario, nao zero a esquerda perdido pela fonte. Padding transformaria
    '1234567' num CNPJ diferente e plausivel.
    """

    value: str

    @field_validator("value", mode="before")
    @classmethod
    def validate_cnpj(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise InvalidIdentifierError(str(v))
        original = v.strip()

        # So digitos e pontuacao de CNPJ. Sem isto, '60872504abc' passaria por
        # ter 8 digitos, e digitos fullwidth (U+FF10..) entrariam pela porta
        # dos fundos -- eles nao casam [0-9], mas int() os converteria.
        if not re.fullmatch(r"[0-9.\-/ ]+", original):
            raise InvalidIdentifierError(original)

        digitos = only_digits(original)
        if len(digitos) == 14:
            if not is_valid_cnpj14(digitos):
                raise InvalidIdentifierError(original)
            return digitos[:8]
        if len(digitos) == 8:
            return digitos
        raise InvalidIdentifierError(original)


class InstitutionList(BaseModel):
    """Normaliza InstitutionInput -> list[str] de CNPJs validados."""

    values: list[str]

    @field_validator("values", mode="before")
    @classmethod
    def normalize_and_validate(cls, v: InstitutionInput) -> list[str]:
        if isinstance(v, str):
            v = [v]
        return [ValidatedCnpj8(value=item).value for item in v]


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
