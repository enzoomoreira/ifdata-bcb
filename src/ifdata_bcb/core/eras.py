"""Deteccao e tratamento de eras de formato do BCB.

O BCB mudou o formato dos dados COSIF ao longo do tempo:
- Era 1 (199501-201009): CSV 8 colunas, CONTA 10 digitos com leading zeros.
- Era 2 (201010-202412): CSV 11 colunas, CONTA 8 digitos.
- Era 3 (202501+): CSV 11 colunas, CONTA 10 digitos (COSIF 1.5).

Eras 1-2 tem codigos de conta compativeis (strip leading zeros).
Era 3 tem codigos incompativeis (novo plano contabil, Resolucao CMN 4.966).

IFDATA Valores (boundary 202503):
- Relatorios contabeis (Resumo, Ativo, Passivo, DRE): codigos renumerados,
  incompativeis entre eras.
- Relatorios de credito (Carteira de credito ativa): codigos estaveis,
  compativeis entre eras. Migraram de TipoInstituicao=2 (financeiro)
  para TipoInstituicao=1 (prudencial).
- Informacoes de Capital: codigos quase identicos (23/24 em comum).
- Relatorio "por nivel de risco da operacao": descontinuado apos 202412.
"""

import unicodedata
from pathlib import Path

from ifdata_bcb.domain.exceptions import (
    DroppedReportWarning,
    IncompatibleEraWarning,
    ScopeMigrationWarning,
)
from ifdata_bcb.infra.log import emit_user_warning

# Primeiro periodo com codigos de conta incompativeis (novo plano contabil).
COSIF_ERA_BOUNDARY: int = 202501
IFDATA_ERA_BOUNDARY: int = 202503

# ---------------------------------------------------------------------------
# Metadados de relatorios IFDATA para verificacao cross-era
# ---------------------------------------------------------------------------

# Prefixo normalizado que identifica relatorios de credito.
_CREDIT_REPORT_PREFIX = "carteira de credito ativa"

# Relatorios com contas estaveis (identicas ou quase identicas entre eras).
# Credit reports sao detectados por prefixo, nao precisam estar aqui.
_STABLE_REPORTS_NORMALIZED: frozenset[str] = frozenset(
    {
        "informacoes de capital",
    }
)

# Relatorios descontinuados: nome normalizado -> ultimo periodo disponivel.
_DROPPED_REPORTS_NORMALIZED: dict[str, int] = {
    "carteira de credito ativa - por nivel de risco da operacao": 202412,
}


def _normalize_report_name(name: str) -> str:
    """Remove acentos, strip e lowercase para matching robusto."""
    return (
        "".join(
            c
            for c in unicodedata.normalize("NFD", name)
            if unicodedata.category(c) != "Mn"
        )
        .strip()
        .lower()
    )


def _is_credit_report(relatorio: str | None) -> bool:
    if relatorio is None:
        return False
    return _normalize_report_name(relatorio).startswith(_CREDIT_REPORT_PREFIX)


def _is_stable_report(relatorio: str | None) -> bool:
    """Report com contas compativeis entre eras (sem renumeracao)."""
    if relatorio is None:
        return False
    norm = _normalize_report_name(relatorio)
    if norm.startswith(_CREDIT_REPORT_PREFIX):
        return True
    return norm in _STABLE_REPORTS_NORMALIZED


def _match_dropped_report(relatorio: str | None) -> int | None:
    """Retorna ultimo periodo disponivel se report foi descontinuado, ou None."""
    if relatorio is None:
        return None
    return _DROPPED_REPORTS_NORMALIZED.get(_normalize_report_name(relatorio))


# ---------------------------------------------------------------------------
# Verificacoes de era
# ---------------------------------------------------------------------------


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
    """Emite IncompatibleEraWarning se dates cruzam o boundary de era.

    Usado pelo COSIF. Para IFDATA, usar check_ifdata_era().
    """
    if dates is None or len(dates) < 2:
        return
    min_date = min(dates)
    max_date = max(dates)
    if min_date < boundary <= max_date:
        emit_user_warning(
            IncompatibleEraWarning(
                f"Query {source_name} abrange periodos antes e apos {boundary}. "
                f"Codigos de conta foram renumerados nesta transicao "
                f"(Resolucao CMN 4.966) e nao sao compativeis entre si. "
                f"Resultados podem misturar contas com codigos distintos.",
                boundary=boundary,
                source=source_name,
            ),
            stacklevel=3,
        )


def check_ifdata_era(
    dates: list[int] | None,
    relatorio: str | None = None,
    escopo: str | None = None,
) -> None:
    """Verificacoes de era especificas para IFDATA Valores.

    Emite ate 3 tipos de warning conforme o cenario:
    - DroppedReportWarning: relatorio descontinuado apos 202412.
    - ScopeMigrationWarning: credit report com escopo filtrado (migracao
      de financeiro para prudencial em 202503).
    - IncompatibleEraWarning: contas renumeradas (apenas para relatorios
      cujos codigos mudaram entre eras).
    """
    if not dates:
        return

    boundary = IFDATA_ERA_BOUNDARY
    max_date = max(dates)
    crosses_boundary = len(dates) >= 2 and min(dates) < boundary <= max_date

    # 1. Report descontinuado (verifica mesmo sem cruzar boundary)
    last_period = _match_dropped_report(relatorio)
    if last_period is not None and max_date > last_period:
        emit_user_warning(
            DroppedReportWarning(
                f"Relatorio '{relatorio}' foi descontinuado apos {last_period}. "
                f"Periodos posteriores nao terao dados para este relatorio.",
                relatorio=relatorio or "",
                last_period=last_period,
            ),
            stacklevel=3,
        )

    if not crosses_boundary:
        return

    # 2. Migracao de escopo: credit reports de financeiro -> prudencial
    if _is_credit_report(relatorio) and escopo in ("financeiro", "prudencial"):
        if escopo == "financeiro":
            gap = f"Periodos >= {boundary} nao terao dados no escopo 'financeiro'"
            alt = "prudencial"
        else:
            gap = f"Periodos < {boundary} nao terao dados no escopo 'prudencial'"
            alt = "financeiro"
        emit_user_warning(
            ScopeMigrationWarning(
                f"Relatorios de credito migraram do escopo 'financeiro' para "
                f"'prudencial' a partir de {boundary}. {gap}. "
                f"Use escopo='{alt}' ou remova o filtro de escopo.",
                relatorio=relatorio or "",
                escopo_pre="financeiro",
                escopo_post="prudencial",
                boundary=boundary,
            ),
            stacklevel=3,
        )

    # 3. IncompatibleEraWarning (apenas se report tem contas renumeradas)
    if not _is_stable_report(relatorio):
        emit_user_warning(
            IncompatibleEraWarning(
                f"Query IFDATA abrange periodos antes e apos {boundary}. "
                f"Codigos de conta foram renumerados nesta transicao "
                f"(Resolucao CMN 4.966) e nao sao compativeis entre si. "
                f"Resultados podem misturar contas com codigos distintos.",
                boundary=boundary,
                source="IFDATA",
            ),
            stacklevel=3,
        )
