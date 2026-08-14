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


class EscopoInfo(TypedDict):
    """Um escopo dentro de describe()['by_escopo']."""

    period_count: int
    has_data: bool


class ExplorerInfo(TypedDict, total=False):
    """Retorno de Explorer.describe().

    `total=False` porque o retorno tem duas formas: com `escopo=` vem a chave
    escopo e os periodos restritos a ele; sem, vem by_escopo com o resumo de
    todos (apenas em explorers com escopos). O restante esta sempre presente.

    Chaves:
        escopos: valores aceitos em escopo=. Vazio quando nao ha escopo.
        columns: colunas listaveis por list_values().
        read_columns: colunas que read() devolve, na ordem.
        filtros: parametros de filtro aceitos por read().
        cadastro_columns: valores aceitos em cadastro=. Vazio se nao aceita.
    """

    escopo: str
    by_escopo: dict[str, EscopoInfo]

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
    "EscopoInfo",
]
