from datetime import date, datetime
from typing import TypedDict

import pandas as pd

# Tipos para parametros de data
# Aceita int, str, date, datetime, pd.Timestamp, ou lista de qualquer um desses
DateScalar = int | str | date | datetime | pd.Timestamp
DateInput = DateScalar | list[DateScalar]

# Tipos para parametros de conta
# Aceita nome unico ou lista de nomes
AccountInput = str | list[str]

# Tipos para parametros de instituicao
# Aceita CNPJ unico ou lista de CNPJs
InstitutionInput = str | list[str]


class SourceInfo(TypedDict):
    """Uma fonte de armazenamento dentro de describe()['by_source']."""

    subdir: str
    prefix: str
    period_count: int
    has_data: bool


class ExplorerInfo(TypedDict, total=False):
    """Retorno de Explorer.describe().

    `total=False` porque o retorno tem duas formas: com `source=` vem
    source/subdir/prefix da fonte pedida; sem, vem sources/by_source com todas.
    O restante das chaves esta sempre presente.

    Chaves:
        escopos: valores aceitos em escopo=. Vazio quando nao ha escopo.
        columns: colunas listaveis por list().
        read_columns: colunas que read() devolve, na ordem.
        filtros: parametros de filtro aceitos por read().
        cadastro_columns: valores aceitos em cadastro=. Vazio se nao aceita.
    """

    source: str
    subdir: str
    prefix: str
    sources: list[str]
    by_source: dict[str, SourceInfo]

    escopos: list[str]
    columns: list[str]
    read_columns: list[str]
    filtros: list[str]
    cadastro_columns: list[str]

    periods: list[int]
    period_count: int
    has_data: bool
    first_period: int | None
    last_period: int | None


__all__ = [
    "DateScalar",
    "DateInput",
    "AccountInput",
    "InstitutionInput",
    "ExplorerInfo",
    "SourceInfo",
]
