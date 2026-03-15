"""Deteccao e tratamento de eras de formato do BCB.

O BCB mudou o formato dos dados COSIF ao longo do tempo:
- Era 1 (199501-201009): CSV 8 colunas, CONTA 10 digitos com leading zeros.
- Era 2 (201010-202412): CSV 11 colunas, CONTA 8 digitos.
- Era 3 (202501+): CSV 11 colunas, CONTA 10 digitos (COSIF 1.5).

Eras 1-2 tem codigos de conta compativeis (strip leading zeros).
Era 3 tem codigos incompativeis (novo plano contabil, Resolucao CMN 4.966).

IFDATA Valores: mesma API OData, mas codigos Conta renumerados em 2025.
"""

import warnings
from pathlib import Path

from ifdata_bcb.domain.exceptions import IncompatibleEraWarning

# Primeiro periodo com codigos de conta incompativeis (novo plano contabil).
COSIF_ERA_BOUNDARY: int = 202501
IFDATA_ERA_BOUNDARY: int = 202503


def detect_cosif_csv_era(csv_path: Path, encoding: str) -> int:
    """Detecta era do CSV COSIF baseado nos headers.

    Retorna 1 (pre-201010, 8 colunas) ou 2 (201010+, 11 colunas).
    Era 3 tem mesma estrutura de colunas que Era 2.
    """
    with open(csv_path, encoding=encoding, errors="replace") as f:
        for _ in range(3):
            f.readline()
        header_line = f.readline()
    if "#DATA_BASE" in header_line:
        return 2
    return 1


def build_cosif_select(era: int, csv_path: Path, encoding: str) -> str:
    """Retorna query SQL que produz schema normalizado independente da era.

    Output uniforme: DATA_BASE, CNPJ, NOME_INSTITUICAO, DOCUMENTO, CONTA,
                     NOME_CONTA, SALDO.
    """
    path_str = str(csv_path).replace("\\", "/")
    if era == 1:
        return f"""
            SELECT
                "DATA" as DATA_BASE,
                CNPJ,
                "NOME INSTITUICAO" as NOME_INSTITUICAO,
                DOCUMENTO,
                CAST(CONTA AS BIGINT) as CONTA,
                UPPER("NOME CONTA") as NOME_CONTA,
                TRY_CAST(REPLACE(SALDO, ',', '.') AS DOUBLE) as SALDO
            FROM read_csv(
                '{path_str}',
                delim=';',
                header=true,
                skip=3,
                encoding='{encoding}'
            )
        """
    return f"""
        SELECT
            "#DATA_BASE" as DATA_BASE,
            CNPJ,
            NOME_INSTITUICAO,
            DOCUMENTO,
            CONTA,
            UPPER(NOME_CONTA) as NOME_CONTA,
            TRY_CAST(REPLACE(SALDO, ',', '.') AS DOUBLE) as SALDO
        FROM read_csv(
            '{path_str}',
            delim=';',
            header=true,
            skip=3,
            encoding='{encoding}'
        )
    """


def check_era_boundary(
    dates: list[int] | None,
    boundary: int,
    source_name: str,
) -> None:
    """Emite IncompatibleEraWarning se dates cruzam o boundary de era."""
    if dates is None or len(dates) < 2:
        return
    min_date = min(dates)
    max_date = max(dates)
    if min_date < boundary <= max_date:
        warnings.warn(
            f"Query {source_name} abrange periodos antes e apos {boundary}. "
            f"Codigos de conta foram renumerados nesta transicao (novo plano "
            f"contabil COSIF 1.5 / Resolucao CMN 4.966) e nao sao compativeis "
            f"entre si. Resultados podem misturar contas com codigos distintos.",
            IncompatibleEraWarning,
            stacklevel=3,
        )
