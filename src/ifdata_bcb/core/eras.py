"""Deteccao e tratamento de eras de formato do BCB.

O BCB mudou o formato dos dados COSIF ao longo do tempo:
- Era 1 (199501-201009): CSV 8 colunas, CONTA 10 digitos com leading zeros.
- Era 2 (201010-202412): CSV 11 colunas, CONTA 8 digitos.
- Era 3 (202501+): CSV 11 colunas, CONTA 10 digitos (COSIF 1.5).

Eras 1-2 tem codigos de conta compativeis (strip leading zeros aplicado na
ingestao). Era 3 tem codigos incompativeis (novo plano contabil, Resolucao
CMN 4.966) e nao ha mapeamento de-para publicado.

A deteccao de incompatibilidade e derivada do dado retornado, nao de tabelas
de metadados: `diagnose_eras()` mede o overlap real de codigos de conta dos
dois lados do boundary. As tabelas deste modulo servem apenas para explicar
*por que* uma lacuna existe, quando a causa e conhecida.
"""

import unicodedata
from pathlib import Path
from typing import Literal, TypedDict

import pandas as pd

from ifdata_bcb.domain.exceptions import (
    DataProcessingError,
    DroppedReportWarning,
    IncompatibleEraWarning,
    PartialDataWarning,
    ScopeMigrationWarning,
)
from ifdata_bcb.infra.log import emit_user_warning

# Primeiro periodo com codigos de conta incompativeis (novo plano contabil).
COSIF_ERA_BOUNDARY: int = 202501
IFDATA_ERA_BOUNDARY: int = 202503

# Marcadores de header que identificam cada era do CSV COSIF.
_ERA2_MARKER = "#DATA_BASE"
_ERA1_MARKERS = ("NOME INSTITUICAO", "NOME CONTA")

# Fracao minima de codigos de conta em comum entre os dois lados do boundary
# para considerar o grupo estavel. Medido nos dados reais de 202412 vs 202503,
# os relatorios se distribuem em 0%, 30%, 60%, 95.8% e 100% -- 0.9 separa
# renumeracao de ajuste pontual sem ambiguidade.
_STABLE_OVERLAP_THRESHOLD = 0.9

# ---------------------------------------------------------------------------
# Metadados de relatorios IFDATA -- usados apenas para explicar lacunas
# ---------------------------------------------------------------------------

# Prefixo normalizado que identifica relatorios de credito.
_CREDIT_REPORT_PREFIX = "carteira de credito ativa"

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


def _match_dropped_report(relatorio: str | None) -> int | None:
    """Retorna ultimo periodo disponivel se report foi descontinuado, ou None."""
    if relatorio is None:
        return None
    return _DROPPED_REPORTS_NORMALIZED.get(_normalize_report_name(relatorio))


# ---------------------------------------------------------------------------
# Deteccao de era no CSV COSIF
# ---------------------------------------------------------------------------


def detect_cosif_csv_era(csv_path: Path, encoding: str) -> int:
    """Detecta era do CSV COSIF baseado nos headers.

    Retorna 1 (pre-201010, 8 colunas) ou 2 (201010+, 11 colunas).
    Era 3 tem mesma estrutura de colunas que Era 2.

    Raises:
        DataProcessingError: se os headers nao correspondem a nenhuma era
            conhecida (formato novo do BCB).
    """
    with open(csv_path, encoding=encoding, errors="replace") as f:
        for _ in range(3):
            f.readline()
        header_line = f.readline()

    if _ERA2_MARKER in header_line:
        return 2

    # Sem esta checagem, um formato novo cairia no SELECT da Era 1 e produziria
    # um Binder Error criptico do DuckDB no lugar de "formato desconhecido".
    if all(marker in header_line for marker in _ERA1_MARKERS):
        return 1

    raise DataProcessingError(
        "cosif",
        f"Formato de CSV desconhecido em {csv_path.name}: os headers nao "
        f"correspondem a nenhuma era conhecida do COSIF. Isso normalmente "
        f"significa que o BCB mudou o layout do arquivo. "
        f"Headers encontrados: {header_line.strip()[:300]}",
    )


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
                TRY_CAST(REPLACE(SALDO, ',', '.') AS DOUBLE) as SALDO,
                SALDO as _saldo_raw
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
            TRY_CAST(REPLACE(SALDO, ',', '.') AS DOUBLE) as SALDO,
            SALDO as _saldo_raw
        FROM read_csv(
            '{path_str}',
            delim=';',
            header=true,
            skip=3,
            encoding='{encoding}'
        )
    """


# ---------------------------------------------------------------------------
# Diagnostico de era derivado do dado retornado
# ---------------------------------------------------------------------------

GrupoStatus = Literal["estavel", "renumerado", "so_pre", "so_post"]


class GrupoEra(TypedDict):
    """Diagnostico de um grupo (relatorio no IFDATA, documento no COSIF)."""

    status: GrupoStatus
    n_pre: int
    n_post: int
    n_comum: int
    pct_overlap: float
    motivo: str | None


class EraDiagnostic(TypedDict):
    """Resultado da analise de era sobre um DataFrame retornado por read()."""

    source: str
    boundary: int
    cruza_boundary: bool
    periodos_solicitados: list[int]
    periodos_presentes: list[int]
    periodos_ausentes: list[int]
    grupos: dict[str, GrupoEra]


def _to_yyyymm(series: pd.Series) -> pd.Series:
    """Converte coluna de data (datetime ou int YYYYMM) para int YYYYMM."""
    if pd.api.types.is_datetime64_any_dtype(series):
        return series.dt.year * 100 + series.dt.month
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _classify(pre: set, post: set) -> tuple[GrupoStatus, float]:
    """Classifica um grupo pelo overlap de contas entre os lados do boundary."""
    if pre and not post:
        return "so_pre", 0.0
    if post and not pre:
        return "so_post", 0.0
    comum = len(pre & post)
    total = max(len(pre), len(post))
    status: GrupoStatus = (
        "estavel" if comum >= total * _STABLE_OVERLAP_THRESHOLD else "renumerado"
    )
    return status, round(comum / total * 100, 1)


def _explain_gap(nome: str, status: GrupoStatus, escopo: str | None) -> str | None:
    """Explica por que um grupo so tem dados de um lado do boundary."""
    if status == "so_pre" and _match_dropped_report(nome) is not None:
        return "descontinuado"
    if _is_credit_report(nome) and escopo in ("financeiro", "prudencial"):
        return "migracao_escopo"
    return None


def diagnose_eras(
    df: pd.DataFrame,
    *,
    boundary: int,
    source: str,
    periodos_solicitados: list[int] | None,
    group_col: str | None = None,
    date_col: str = "data",
    account_col: str = "cod_conta",
    escopo: str | None = None,
) -> EraDiagnostic:
    """Analisa continuidade dos dados retornados atraves do boundary de era.

    Mede o overlap real de codigos de conta dos dois lados do boundary, por
    grupo (relatorio no IFDATA, documento no COSIF). Nao consulta tabela de
    metadados para decidir: o veredito vem do dado.

    O diagnostico degrada graciosamente quando o chamador restringiu `columns`:
    sem `group_col` a analise e global; sem `account_col` fica apenas a
    cobertura de periodos.

    Args:
        df: Resultado de read(), apos _finalize_read e antes de _filter_columns.
        boundary: Primeiro periodo (YYYYMM) da era nova.
        source: Nome da fonte para as mensagens ("COSIF", "IFDATA").
        periodos_solicitados: Range pedido pelo usuario, para detectar lacuna.
        group_col: Coluna de agrupamento. None analisa o DataFrame inteiro.
        date_col: Coluna de data (datetime ou int YYYYMM).
        account_col: Coluna com o codigo de conta.
        escopo: Escopo filtrado, usado para explicar migracao de escopo.
    """
    solicitados = sorted(periodos_solicitados or [])
    diag: EraDiagnostic = {
        "source": source,
        "boundary": boundary,
        "cruza_boundary": False,
        "periodos_solicitados": solicitados,
        "periodos_presentes": [],
        "periodos_ausentes": [],
        "grupos": {},
    }

    if df.empty or date_col not in df.columns:
        return diag

    periodos = _to_yyyymm(df[date_col])
    presentes = sorted({int(p) for p in periodos.dropna().unique()})
    diag["periodos_presentes"] = presentes
    diag["periodos_ausentes"] = sorted(set(solicitados) - set(presentes))

    # A analise so faz sentido se o usuario pediu dados dos dois lados. Quando
    # pediu um lado so, uma lacuna nao tem relacao com a mudanca de era.
    pediu_pre = any(p < boundary for p in solicitados)
    pediu_post = any(p >= boundary for p in solicitados)
    if not (pediu_pre and pediu_post):
        return diag

    diag["cruza_boundary"] = True

    if account_col not in df.columns:
        return diag

    cols = [date_col, account_col] + ([group_col] if group_col in df.columns else [])
    slim = df[cols].drop_duplicates()
    slim = slim.assign(_pre=_to_yyyymm(slim[date_col]) < boundary)

    grupos = (
        slim.groupby(group_col, sort=False)
        if group_col in df.columns
        else [(source, slim)]
    )
    for nome, g in grupos:
        pre = set(g.loc[g["_pre"], account_col])
        post = set(g.loc[~g["_pre"], account_col])
        if not pre and not post:
            continue
        status, pct = _classify(pre, post)
        diag["grupos"][str(nome)] = {
            "status": status,
            "n_pre": len(pre),
            "n_post": len(post),
            "n_comum": len(pre & post),
            "pct_overlap": pct,
            "motivo": _explain_gap(str(nome), status, escopo),
        }
    return diag


# ---------------------------------------------------------------------------
# Emissao de warnings a partir do diagnostico
# ---------------------------------------------------------------------------


def check_dropped_report(
    relatorio: str | None,
    periodos: list[int] | None,
    *,
    stacklevel: int = 3,
) -> None:
    """Avisa quando o filtro pede um relatorio que o BCB ja descontinuou.

    Complementa `diagnose_eras()`: com resultado vazio nao ha dado para medir,
    mas o nome do relatorio filtrado ja explica o vazio.
    """
    last_period = _match_dropped_report(relatorio)
    if last_period is None or not periodos or max(periodos) <= last_period:
        return
    emit_user_warning(
        DroppedReportWarning(
            f"Relatorio '{relatorio}' foi descontinuado apos {last_period}. "
            f"Periodos posteriores nao terao dados para este relatorio.",
            relatorio=relatorio or "",
            last_period=last_period,
        ),
        stacklevel=stacklevel,
    )


def _join(nomes: list[str], limit: int = 5) -> str:
    """Lista nomes para mensagem, resumindo quando forem muitos."""
    if len(nomes) <= limit:
        return ", ".join(repr(n) for n in nomes)
    head = ", ".join(repr(n) for n in nomes[:limit])
    return f"{head} e mais {len(nomes) - limit}"


def emit_era_warnings(diag: EraDiagnostic, *, stacklevel: int = 3) -> None:
    """Emite warnings a partir de um EraDiagnostic.

    Grupos com a mesma causa saem num warning agregado -- um resultado bulk
    cruzando o boundary tem dezenas de grupos e um warning por grupo seria
    ruido, nao sinal.
    """
    if not diag["cruza_boundary"]:
        return

    boundary = diag["boundary"]
    source = diag["source"]

    renumerados = {
        nome: g for nome, g in diag["grupos"].items() if g["status"] == "renumerado"
    }
    if renumerados:
        detalhe = _join(
            [
                f"{nome} ({g['pct_overlap']}% dos codigos em comum)"
                for nome, g in sorted(renumerados.items())
            ]
        )
        emit_user_warning(
            IncompatibleEraWarning(
                f"Os codigos de conta {source} foram renumerados em {boundary} "
                f"(Resolucao CMN 4.966). A serie de cada conta termina em "
                f"{boundary} e recomeca com outro codigo -- trate os periodos "
                f"antes e depois como duas series distintas, nao como uma "
                f"continua. Afetados: {detalhe}.",
                boundary=boundary,
                source=source,
            ),
            stacklevel=stacklevel,
        )

    parciais = [
        (nome, g)
        for nome, g in sorted(diag["grupos"].items())
        if g["status"] in ("so_pre", "so_post")
    ]

    for nome, grupo in parciais:
        if grupo["motivo"] == "descontinuado":
            last_period = _match_dropped_report(nome)
            emit_user_warning(
                DroppedReportWarning(
                    f"Relatorio '{nome}' foi descontinuado apos {last_period}. "
                    f"Periodos posteriores nao terao dados para este relatorio.",
                    relatorio=nome,
                    last_period=last_period or boundary,
                ),
                stacklevel=stacklevel,
            )

    migrados = [(n, g) for n, g in parciais if g["motivo"] == "migracao_escopo"]
    if migrados:
        if migrados[0][1]["status"] == "so_pre":
            gap = f"Periodos >= {boundary} nao terao dados no escopo 'financeiro'"
            alt = "prudencial"
        else:
            gap = f"Periodos < {boundary} nao terao dados no escopo 'prudencial'"
            alt = "financeiro"
        emit_user_warning(
            ScopeMigrationWarning(
                f"Relatorios de credito migraram do escopo 'financeiro' para "
                f"'prudencial' a partir de {boundary}. {gap}. "
                f"Use escopo='{alt}' ou remova o filtro de escopo. "
                f"Afetados: {_join([n for n, _ in migrados])}.",
                relatorio=migrados[0][0],
                escopo_pre="financeiro",
                escopo_post="prudencial",
                boundary=boundary,
            ),
            stacklevel=stacklevel,
        )

    sem_explicacao = [(n, g) for n, g in parciais if g["motivo"] is None]
    if sem_explicacao:
        so_post = [n for n, g in sem_explicacao if g["status"] == "so_post"]
        so_pre = [n for n, g in sem_explicacao if g["status"] == "so_pre"]
        faltas = []
        if so_post:
            faltas.append(f"sem dados antes de {boundary}: {_join(so_post)}")
        if so_pre:
            faltas.append(f"sem dados a partir de {boundary}: {_join(so_pre)}")
        emit_user_warning(
            PartialDataWarning(
                f"O periodo solicitado cruza a mudanca de era de {boundary}, mas "
                f"parte do resultado cobre apenas um dos lados ({'; '.join(faltas)}). "
                f"Pode ser mudanca do BCB nesta transicao ou cache incompleto -- "
                f"confira com {source.lower()}.collect().",
                reason="era_coverage_gap",
                detail={"so_pre": so_pre, "so_post": so_post, "boundary": boundary},
            ),
            stacklevel=stacklevel,
        )
