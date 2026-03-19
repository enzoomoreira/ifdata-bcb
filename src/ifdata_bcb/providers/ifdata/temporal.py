"""Resolucao temporal de CNPJs para codigos IFDATA por periodo."""

from dataclasses import dataclass, field

import pandas as pd

from ifdata_bcb.core.constants import (
    TIPO_INST_MAP,
    get_pattern,
    get_subdir,
)
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.validation import NormalizedDates
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import build_in_clause, build_int_condition


@dataclass(frozen=True)
class TemporalGroup:
    """Grupo de periodos com mesmo cod_inst para um escopo."""

    cod_inst: str
    tipo_inst: int
    periodos: list[int] = field(default_factory=list)
    cnpj_map: dict[str, list[str]] = field(default_factory=dict)


_ESCOPO_TO_COD_COL: dict[str, str] = {
    "prudencial": "CodConglomeradoPrudencial",
    "financeiro": "CodConglomeradoFinanceiro",
}

_EMPTY_MAPEAMENTO_COLUMNS = [
    "COD_INST",
    "TIPO_INST",
    "ESCOPO",
    "REPORT_KEY_TYPE",
    "CNPJ_8",
    "INSTITUICAO",
]


def _resolve_quarter_dates(start: str | None, end: str | None) -> list[int] | None:
    """Converte start/end em lista de YYYYMM trimestrais."""
    if start is None:
        return None
    from ifdata_bcb.utils.date import align_to_quarter_end

    start_norm = NormalizedDates(values=start).values[0]
    start_q = align_to_quarter_end(start_norm)
    if end is None:
        return [start_q]
    from ifdata_bcb.utils.date import generate_quarter_range

    return generate_quarter_range(start, end)


class TemporalResolver:
    """Resolve CNPJs para codigos IFDATA por periodo usando cadastro temporal."""

    def __init__(
        self,
        query_engine: QueryEngine,
        entity_lookup: EntityLookup,
        valores_subdir: str,
        valores_pattern: str,
    ):
        self._qe = query_engine
        self._resolver = entity_lookup
        self._valores_subdir = valores_subdir
        self._valores_pattern = valores_pattern
        self._logger = get_logger(__name__)

    def _ifdata_date_where(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> str:
        """Constroi WHERE clause para datas AnoMes (trimestral)."""
        periodos = _resolve_quarter_dates(start, end)
        if not periodos:
            return ""
        return f"WHERE {build_int_condition('AnoMes', periodos)}"

    def resolve(
        self,
        cnpjs: list[str],
        escopo: str,
        periodos: list[int],
    ) -> tuple[list[TemporalGroup], list[str]]:
        """Resolve CNPJs para codigos IFDATA por periodo.

        Retorna (groups, unavailable_cnpjs).
        """
        if not cnpjs or not periodos:
            return [], []

        tipo_inst = TIPO_INST_MAP[escopo]

        if escopo == "individual":
            cnpj_map = {cnpj: [cnpj] for cnpj in cnpjs}
            return (
                [
                    TemporalGroup(
                        cod_inst="_individual_",
                        tipo_inst=tipo_inst,
                        periodos=periodos,
                        cnpj_map=cnpj_map,
                    )
                ],
                [],
            )

        cod_col = _ESCOPO_TO_COD_COL[escopo]
        cadastro_pattern = get_pattern("cadastro")
        cadastro_subdir = get_subdir("cadastro")

        if not self._qe.has_glob(cadastro_pattern, cadastro_subdir):
            from ifdata_bcb.infra.log import emit_user_warning
            from ifdata_bcb.domain.exceptions import PartialDataWarning

            emit_user_warning(
                PartialDataWarning(
                    "Cadastro nao encontrado no cache. "
                    "Resolucao de conglomerado indisponivel. "
                    "Execute cadastro.collect() para baixar os dados.",
                    reason="cadastro_missing",
                ),
                stacklevel=3,
            )
            return [], []

        path = self._qe.cache_path / cadastro_subdir / cadastro_pattern
        cnpjs_str = build_in_clause(cnpjs)

        query = f"""
            SELECT CNPJ_8, Data, {cod_col} as cod
            FROM read_parquet('{path}', union_by_name=true)
            WHERE CNPJ_8 IN ({cnpjs_str})
              AND {self._resolver.real_entity_condition()}
              AND {cod_col} IS NOT NULL
            ORDER BY CNPJ_8, Data
        """
        try:
            df_cad = self._qe.sql(query)
        except Exception as e:
            self._logger.warning(f"Temporal resolution query failed: {e}")
            from ifdata_bcb.infra.log import emit_user_warning
            from ifdata_bcb.domain.exceptions import PartialDataWarning

            emit_user_warning(
                PartialDataWarning(
                    f"Resolucao temporal falhou: {e}. Resultados podem estar incompletos.",
                    reason="temporal_query_failed",
                ),
                stacklevel=3,
            )
            return [], []

        if df_cad.empty:
            return [], cnpjs[:]

        # Construir mapa: cnpj -> [(data_cadastro, cod)] via arrays (sem iterrows)
        cnpj_history: dict[str, list[tuple[int, str]]] = {}
        cnpjs_arr = df_cad["CNPJ_8"].astype(str).values
        datas_arr = df_cad["Data"].values
        cods_arr = df_cad["cod"].astype(str).values
        for cnpj, data, cod in zip(cnpjs_arr, datas_arr, cods_arr):
            cnpj_history.setdefault(cnpj, []).append((int(data), cod))

        groups: dict[str, dict] = {}
        unavailable_cnpjs: list[str] = []

        for cnpj in cnpjs:
            history = cnpj_history.get(cnpj)
            if not history:
                unavailable_cnpjs.append(cnpj)
                continue

            cnpj_resolved_any = False
            for periodo in periodos:
                # Backfill: ultimo cadastro <= periodo
                cod = None
                for data_cad, cod_cad in reversed(history):
                    if data_cad <= periodo:
                        cod = cod_cad
                        break

                if cod is None:
                    # Forward fill
                    cod = history[0][1]

                if cod:
                    cnpj_resolved_any = True
                    if cod not in groups:
                        groups[cod] = {"periodos": set(), "cnpj_map": {}}
                    groups[cod]["periodos"].add(periodo)
                    groups[cod]["cnpj_map"].setdefault(cod, [])
                    if cnpj not in groups[cod]["cnpj_map"].setdefault(cod, []):
                        groups[cod]["cnpj_map"][cod].append(cnpj)

            if not cnpj_resolved_any:
                unavailable_cnpjs.append(cnpj)

        return (
            [
                TemporalGroup(
                    cod_inst=cod,
                    tipo_inst=tipo_inst,
                    periodos=sorted(g["periodos"]),
                    cnpj_map=g["cnpj_map"],
                )
                for cod, g in groups.items()
            ],
            unavailable_cnpjs,
        )

    def resolve_mapeamento(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Junta reporters com cadastro via SQL, resolve lookup por escopo."""
        valores_path = (
            self._qe.cache_path / self._valores_subdir / self._valores_pattern
        )
        if not self._qe.has_glob(self._valores_pattern, self._valores_subdir):
            return pd.DataFrame(columns=_EMPTY_MAPEAMENTO_COLUMNS)

        cadastro_pattern = get_pattern("cadastro")
        cadastro_subdir = get_subdir("cadastro")
        if not self._qe.has_glob(cadastro_pattern, cadastro_subdir):
            return pd.DataFrame(columns=_EMPTY_MAPEAMENTO_COLUMNS)

        cadastro_path = self._qe.cache_path / cadastro_subdir / cadastro_pattern

        # Filtros de data
        valores_where = self._ifdata_date_where(start, end)
        cadastro_where_parts = [self._resolver.real_entity_condition()]
        periodos = _resolve_quarter_dates(start, end)
        if periodos:
            cadastro_where_parts.append(build_int_condition("Data", periodos))
        cadastro_where = " AND ".join(cadastro_where_parts)

        tipo_ind = TIPO_INST_MAP["individual"]
        tipo_prud = TIPO_INST_MAP["prudencial"]
        tipo_fin = TIPO_INST_MAP["financeiro"]

        query = f"""
            WITH reporters AS (
                SELECT DISTINCT CodInst as COD_INST, TipoInstituicao as TIPO_INST
                FROM '{valores_path}'
                {valores_where}
            ),
            cadastro AS (
                SELECT CNPJ_8, NomeInstituicao as INSTITUICAO,
                       CodConglomeradoPrudencial as COD_CONGL_PRUD,
                       CodConglomeradoFinanceiro as COD_CONGL_FIN
                FROM (
                    SELECT CNPJ_8, NomeInstituicao,
                           CodConglomeradoPrudencial, CodConglomeradoFinanceiro,
                           ROW_NUMBER() OVER (
                               PARTITION BY CNPJ_8 ORDER BY Data DESC
                           ) as rn
                    FROM read_parquet('{cadastro_path}', union_by_name=true)
                    WHERE {cadastro_where}
                )
                WHERE rn = 1
            )
            SELECT DISTINCT COD_INST, TIPO_INST, ESCOPO, REPORT_KEY_TYPE,
                            CNPJ_8, INSTITUICAO
            FROM (
                SELECT r.COD_INST, r.TIPO_INST, 'individual' as ESCOPO,
                       'cnpj' as REPORT_KEY_TYPE,
                       r.COD_INST as CNPJ_8, c.INSTITUICAO
                FROM reporters r
                LEFT JOIN cadastro c ON r.COD_INST = c.CNPJ_8
                WHERE r.TIPO_INST = {tipo_ind}

                UNION ALL

                SELECT r.COD_INST, r.TIPO_INST, 'prudencial' as ESCOPO,
                       CASE WHEN r.COD_INST = c.CNPJ_8
                            THEN 'cnpj' ELSE 'prudencial' END as REPORT_KEY_TYPE,
                       c.CNPJ_8, c.INSTITUICAO
                FROM reporters r
                JOIN cadastro c
                    ON (r.COD_INST = c.CNPJ_8 OR r.COD_INST = c.COD_CONGL_PRUD)
                WHERE r.TIPO_INST = {tipo_prud}

                UNION ALL

                SELECT r.COD_INST, r.TIPO_INST, 'financeiro' as ESCOPO,
                       CASE WHEN r.COD_INST = c.CNPJ_8
                            THEN 'cnpj' ELSE 'financeiro' END as REPORT_KEY_TYPE,
                       c.CNPJ_8, c.INSTITUICAO
                FROM reporters r
                JOIN cadastro c
                    ON (r.COD_INST = c.CNPJ_8 OR r.COD_INST = c.COD_CONGL_FIN)
                WHERE r.TIPO_INST = {tipo_fin}
            )
            ORDER BY COD_INST, TIPO_INST, CNPJ_8
        """

        try:
            df = self._qe.sql(query)
        except Exception as e:
            self._logger.warning(f"resolve_mapeamento query failed: {e}")
            return pd.DataFrame(columns=_EMPTY_MAPEAMENTO_COLUMNS)

        if df.empty:
            return pd.DataFrame(columns=_EMPTY_MAPEAMENTO_COLUMNS)

        return df.reset_index(drop=True)

    @staticmethod
    def add_cnpj_mapping(
        df: pd.DataFrame,
        cnpj_map: dict[str, list[str]],
        cod_inst_col: str = "CodInst",
    ) -> pd.DataFrame:
        """Adiciona CNPJ_8 via merge com mapa cod_inst -> cnpjs."""
        if df.empty:
            return df

        if not cnpj_map:
            df = df.copy()
            df["CNPJ_8"] = df[cod_inst_col]
            return df

        rows = [
            {cod_inst_col: cod, "CNPJ_8": cnpj}
            for cod, cnpjs in cnpj_map.items()
            for cnpj in cnpjs
        ]
        df_map = pd.DataFrame(rows)
        return df.merge(df_map, on=cod_inst_col, how="left")
