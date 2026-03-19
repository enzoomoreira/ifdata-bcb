"""Explorer para dados cadastrais IFDATA (trimestrais)."""

import pandas as pd

from ifdata_bcb.core.constants import DATA_SOURCES, get_subdir
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import MissingRequiredParameterError
from ifdata_bcb.domain.types import InstitutionInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import (
    build_int_condition,
    build_string_condition,
    join_conditions,
)
from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.providers.ifdata.cadastro.collector import IFDATACadastroCollector


class CadastroExplorer(BaseExplorer):
    """Explorer para dados cadastrais IFDATA (trimestrais)."""

    _DROP_COLUMNS = ["CodInst"]
    _PASSTHROUGH_COLUMNS: set[str] = {"CNPJ_8", "CNPJ_LIDER_8"}

    _DATE_COLUMN = "Data"

    _COLUMN_MAP = {
        "Data": "DATA",
        "NomeInstituicao": "INSTITUICAO",
        "SegmentoTb": "SEGMENTO",
        "CodConglomeradoPrudencial": "COD_CONGL_PRUD",
        "CodConglomeradoFinanceiro": "COD_CONGL_FIN",
        "Situacao": "SITUACAO",
        "Atividade": "ATIVIDADE",
        "Tcb": "TCB",
        "Td": "TD",
        "Tc": "TC",
        "Uf": "UF",
        "Municipio": "MUNICIPIO",
        "Sr": "SR",
        "DataInicioAtividade": "DATA_INICIO_ATIVIDADE",
    }

    _COLUMN_ORDER = [
        "DATA",
        "CNPJ_8",
        "INSTITUICAO",
        "SEGMENTO",
        "COD_CONGL_PRUD",
        "COD_CONGL_FIN",
        "CNPJ_LIDER_8",
        "SITUACAO",
        "ATIVIDADE",
        "TCB",
        "TD",
        "TC",
        "UF",
        "MUNICIPIO",
        "SR",
        "DATA_INICIO_ATIVIDADE",
    ]

    def __init__(
        self,
        query_engine: QueryEngine | None = None,
        entity_lookup: EntityLookup | None = None,
    ):
        super().__init__(query_engine, entity_lookup)
        self._collector: IFDATACadastroCollector | None = None

    def _get_subdir(self) -> str:
        return get_subdir("cadastro")

    def _get_file_prefix(self) -> str:
        return DATA_SOURCES["cadastro"]["prefix"]

    def _get_collector(self) -> IFDATACadastroCollector:
        if self._collector is None:
            self._collector = IFDATACadastroCollector()
        return self._collector

    def _build_real_entidade_condition(self) -> str:
        return self._resolver.real_entity_condition()

    def collect(
        self, start: str, end: str, force: bool = False, verbose: bool = True
    ) -> None:
        """Coleta dados cadastrais IFDATA do BCB (trimestral)."""
        self._get_collector().collect(start, end, force=force, verbose=verbose)

    def read(
        self,
        start: str,
        end: str | None = None,
        *,
        instituicao: InstitutionInput | None = None,
        segmento: str | None = None,
        uf: str | None = None,
        situacao: str | None = None,
        atividade: str | None = None,
        tcb: str | None = None,
        td: str | None = None,
        tc: str | int | None = None,
        sr: str | None = None,
        municipio: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Le dados cadastrais com filtros.

        Args:
            start: Periodo inicial (obrigatorio). Formato: '2024-12' ou '202412'.
            end: Periodo final. Se None, retorna apenas start.
            instituicao: CNPJ de 8 digitos. Se None, retorna todas.
            segmento: Filtro por segmento (case/accent insensitive).
            uf: Filtro por UF (case/accent insensitive).
            situacao: Filtro por situacao (case/accent insensitive).
            atividade: Filtro por atividade (case/accent insensitive).
            tcb: Filtro por TCB (case/accent insensitive).
            td: Filtro por TD (case/accent insensitive).
            tc: Filtro por TC (aceita str ou int).
            sr: Filtro por SR (case/accent insensitive).
            municipio: Filtro por municipio (case/accent insensitive).
            columns: Colunas a retornar. Se None, retorna todas.

        Raises:
            MissingRequiredParameterError: Se start nao fornecido.
            InvalidDateRangeError: Se start > end.
        """
        self._validate_required_params(start)
        columns = self._validate_columns(columns)
        self._logger.debug(
            f"Cadastro read: instituicao={instituicao}, segmento={segmento}"
        )

        conditions = [
            self._build_cnpj_condition(instituicao),
            self._build_date_condition(start, end, trimestral=True),
            self._build_real_entidade_condition(),
        ]

        filter_params: dict[str, str | int | None] = {
            "SEGMENTO": segmento,
            "UF": uf,
            "SITUACAO": situacao,
            "ATIVIDADE": atividade,
            "TCB": tcb,
            "TD": td,
            "TC": tc,
            "SR": sr,
            "MUNICIPIO": municipio,
        }

        for col_name, value in filter_params.items():
            if value is not None:
                conditions.append(
                    build_string_condition(
                        self._storage_col(col_name),
                        [str(value)],
                        case_insensitive=True,
                        accent_insensitive=True,
                    )
                )

        df = self._read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            columns=self._translate_columns(columns),
            where=join_conditions(conditions),
        )
        return self._finalize_read(df)

    def _resolve_start_fallback(self, start: str | None) -> str:
        """Resolve start: se None, usa ultimo periodo disponivel."""
        if start is not None:
            return start
        latest = self._get_latest_periodo()
        if latest is None:
            raise MissingRequiredParameterError("start (nenhum dado disponivel)")
        return str(latest)

    def info(self, instituicao: str, start: str | None = None) -> dict | None:
        """
        Retorna dict com info da instituicao no periodo especificado.
        Se start=None, usa ultimo periodo. Retorna None se nao encontrar.
        """
        start = self._resolve_start_fallback(start)
        cnpj = self._resolve_entidade(instituicao)
        df = self.read(start, instituicao=cnpj)

        if df.empty:
            self._logger.warning(f"Institution not found: {instituicao}")
            return None

        row = df.iloc[0]
        result = row.to_dict()

        for key, value in result.items():
            if value == "null":
                result[key] = None

        return result

    def list_segmentos(self) -> list[str]:
        """Lista segmentos disponiveis."""
        if not self._ensure_data_exists():
            return []

        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()
        query = f"""
            SELECT DISTINCT SegmentoTb as SEGMENTO
            FROM '{path}'
            WHERE SegmentoTb IS NOT NULL
              AND {self._build_real_entidade_condition()}
            ORDER BY SEGMENTO
        """
        df = self._qe.sql(query)
        return df["SEGMENTO"].tolist() if not df.empty else []

    def list_ufs(self) -> list[str]:
        """Lista UFs disponiveis."""
        if not self._ensure_data_exists():
            return []

        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()
        query = f"""
            SELECT DISTINCT Uf as UF
            FROM '{path}'
            WHERE Uf IS NOT NULL
              AND {self._build_real_entidade_condition()}
            ORDER BY UF
        """
        df = self._qe.sql(query)
        return df["UF"].tolist() if not df.empty else []

    def get_conglomerate_members(
        self, cod_congl: str, start: str | None = None
    ) -> pd.DataFrame:
        """
        Retorna membros de um conglomerado prudencial.
        Se start=None, usa ultimo periodo.
        """
        start = self._resolve_start_fallback(start)

        data = self._align_to_quarter_end(self._normalize_datas(start)[0])

        conditions = [
            build_string_condition(self._storage_col("COD_CONGL_PRUD"), [cod_congl]),
            build_int_condition(self._storage_col("DATA"), [data]),
            self._build_real_entidade_condition(),
        ]

        df = self._read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            where=join_conditions(conditions),
        )
        return self._finalize_read(df)
