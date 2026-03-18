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


_TIPO_INST_REVERSE = {v: k for k, v in TIPO_INST_MAP.items()}

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
                "Cadastro nao encontrado no cache. "
                "Resolucao de conglomerado indisponivel. "
                "Execute cadastro.collect() para baixar os dados.",
                PartialDataWarning,
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
                f"Resolucao temporal falhou: {e}. Resultados podem estar incompletos.",
                PartialDataWarning,
                stacklevel=3,
            )
            return [], []

        if df_cad.empty:
            return [], cnpjs[:]

        # Construir mapa: cnpj -> [(data_cadastro, cod)]
        cnpj_history: dict[str, list[tuple[int, str]]] = {}
        for _, row in df_cad.iterrows():
            cnpj = str(row["CNPJ_8"])
            data = int(row["Data"])
            cod = str(row["cod"])
            cnpj_history.setdefault(cnpj, []).append((data, cod))

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

    def load_mapeamento_rows(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Lista pares cod_inst/tipo_inst nos dados IFDATA."""
        if not self._qe.has_glob(self._valores_pattern, self._valores_subdir):
            return pd.DataFrame(columns=["COD_INST", "TIPO_INST", "ESCOPO"])

        path = self._qe.cache_path / self._valores_subdir / self._valores_pattern
        where = self._ifdata_date_where(start, end)
        query = f"""
            SELECT DISTINCT CodInst as COD_INST, TipoInstituicao as TIPO_INST
            FROM '{path}'
            {where}
            ORDER BY COD_INST, TIPO_INST
        """
        df = self._qe.sql(query)
        if df.empty:
            return df
        df["ESCOPO"] = df["TIPO_INST"].map(_TIPO_INST_REVERSE)
        return df

    def load_cadastro_entities(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Carrega entidades com codigos de conglomerado."""
        cadastro_pattern = get_pattern("cadastro")
        cadastro_subdir = get_subdir("cadastro")
        if not self._qe.has_glob(cadastro_pattern, cadastro_subdir):
            return pd.DataFrame(
                columns=["CNPJ_8", "INSTITUICAO", "COD_CONGL_PRUD", "COD_CONGL_FIN"]
            )

        path = self._qe.cache_path / cadastro_subdir / cadastro_pattern
        where_parts = [self._resolver.real_entity_condition()]
        periodos = _resolve_quarter_dates(start, end)
        if periodos:
            where_parts.append(build_int_condition("Data", periodos))
        where = " AND ".join(where_parts)
        query = f"""
            SELECT
                CNPJ_8,
                NomeInstituicao as INSTITUICAO,
                CodConglomeradoPrudencial as COD_CONGL_PRUD,
                CodConglomeradoFinanceiro as COD_CONGL_FIN
            FROM (
                SELECT
                    CNPJ_8,
                    NomeInstituicao,
                    CodConglomeradoPrudencial,
                    CodConglomeradoFinanceiro,
                    ROW_NUMBER() OVER (
                        PARTITION BY CNPJ_8
                        ORDER BY Data DESC
                    ) as rn
                FROM read_parquet('{path}', union_by_name=true)
                WHERE {where}
            )
            WHERE rn = 1
        """
        return self._qe.sql(query)

    def resolve_mapeamento(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Junta reporters com cadastro, resolve lookup."""
        reporters = self.load_mapeamento_rows(start, end)
        if reporters.empty:
            return pd.DataFrame(columns=_EMPTY_MAPEAMENTO_COLUMNS)

        cadastro = self.load_cadastro_entities(start, end)
        frames: list[pd.DataFrame] = []

        individual = reporters[
            reporters["TIPO_INST"] == TIPO_INST_MAP["individual"]
        ].copy()
        if not individual.empty:
            individual["CNPJ_8"] = individual["COD_INST"]
            individual["REPORT_KEY_TYPE"] = "cnpj"
            frames.append(individual)

        for escopo, tipo_inst, cod_col in [
            ("prudencial", TIPO_INST_MAP["prudencial"], "COD_CONGL_PRUD"),
            ("financeiro", TIPO_INST_MAP["financeiro"], "COD_CONGL_FIN"),
        ]:
            subset = reporters[reporters["TIPO_INST"] == tipo_inst].copy()
            if subset.empty:
                continue

            lookup = pd.concat(
                [
                    cadastro[["CNPJ_8"]].assign(COD_INST=cadastro["CNPJ_8"]),
                    cadastro[["CNPJ_8", cod_col]]
                    .rename(columns={cod_col: "COD_INST"})
                    .dropna(subset=["COD_INST"]),
                ],
                ignore_index=True,
            ).drop_duplicates()

            merged = subset.merge(lookup, on="COD_INST", how="left")
            merged = merged.dropna(subset=["CNPJ_8"])
            if merged.empty:
                continue
            merged["REPORT_KEY_TYPE"] = (
                merged["COD_INST"]
                .astype(str)
                .eq(merged["CNPJ_8"].astype(str))
                .map({True: "cnpj", False: escopo})
            )
            frames.append(merged)

        if not frames:
            return pd.DataFrame(columns=_EMPTY_MAPEAMENTO_COLUMNS)

        df = pd.concat(frames, ignore_index=True)

        # Aplica nomes canonicos
        cnpjs = df["CNPJ_8"].dropna().astype(str).unique().tolist()
        if cnpjs:
            nomes = self._resolver.get_canonical_names_for_cnpjs(cnpjs)
            df["INSTITUICAO"] = df["CNPJ_8"].astype(str).map(nomes)

        return (
            df[
                [
                    "COD_INST",
                    "TIPO_INST",
                    "ESCOPO",
                    "REPORT_KEY_TYPE",
                    "CNPJ_8",
                    "INSTITUICAO",
                ]
            ]
            .drop_duplicates()
            .sort_values(["COD_INST", "TIPO_INST", "CNPJ_8"])
            .reset_index(drop=True)
        )

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
            df["CNPJ_8"] = df[cod_inst_col]
            return df

        rows = [
            {cod_inst_col: cod, "CNPJ_8": cnpj}
            for cod, cnpjs in cnpj_map.items()
            for cnpj in cnpjs
        ]
        df_map = pd.DataFrame(rows)
        return df.merge(df_map, on=cod_inst_col, how="left")
