from typing import Optional

import pandas as pd

from ifdata_bcb.core.base_explorer import BaseExplorer
from ifdata_bcb.core.constants import DATA_SOURCES, get_subdir
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import MissingRequiredParameterError
from ifdata_bcb.domain.types import InstitutionInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.ifdata.collector import IFDATACadastroCollector


class CadastroExplorer(BaseExplorer):
    """Explorer para dados cadastrais IFDATA (trimestrais)."""

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

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        entity_lookup: Optional[EntityLookup] = None,
    ):
        super().__init__(query_engine, entity_lookup)
        self._collector: Optional[IFDATACadastroCollector] = None

    def _get_subdir(self) -> str:
        return get_subdir("cadastro")

    def _get_file_prefix(self) -> str:
        return DATA_SOURCES["cadastro"]["prefix"]

    def _get_pattern(self) -> str:
        return f"{self._get_file_prefix()}_*.parquet"

    def _get_collector(self) -> IFDATACadastroCollector:
        if self._collector is None:
            self._collector = IFDATACadastroCollector()
        return self._collector

    def collect(
        self, start: str, end: str, force: bool = False, verbose: bool = True
    ) -> None:
        """Coleta dados cadastrais IFDATA do BCB (trimestral)."""
        self._get_collector().collect(start, end, force=force, verbose=verbose)

    def read(
        self,
        instituicao: InstitutionInput,
        start: str,
        end: Optional[str] = None,
        segmento: Optional[str] = None,
        uf: Optional[str] = None,
        columns: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Le dados cadastrais com filtros."""
        self._validate_required_params(instituicao, start)
        self._logger.debug(
            f"Cadastro read: instituicao={instituicao}, segmento={segmento}"
        )

        conditions = [
            self._build_cnpj_condition(instituicao),
            self._build_date_condition(start, end, trimestral=True),
        ]

        if segmento:
            conditions.append(
                self._build_string_condition(
                    self._storage_col("SEGMENTO"), [segmento], case_insensitive=True
                )
            )

        if uf:
            conditions.append(
                self._build_string_condition(
                    self._storage_col("UF"), [uf], case_insensitive=True
                )
            )

        df = self._qe.read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            columns=columns,
            where=self._join_conditions(conditions),
        )
        return self._finalize_read(df)

    def info(self, instituicao: str, start: str) -> Optional[dict]:
        """
        Retorna dict com info da instituicao no periodo especificado.
        Retorna None se nao encontrar.
        """
        cnpj = self._resolve_entity(instituicao)
        df = self.read(instituicao=cnpj, start=start)

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
        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()
        query = f"""
            SELECT DISTINCT SegmentoTb as SEGMENTO
            FROM '{path}'
            WHERE SegmentoTb IS NOT NULL
            ORDER BY SEGMENTO
        """
        df = self._qe.sql(query)
        return df["SEGMENTO"].tolist() if not df.empty else []

    def list_ufs(self) -> list[str]:
        """Lista UFs disponiveis."""
        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()
        query = f"""
            SELECT DISTINCT Uf as UF
            FROM '{path}'
            WHERE Uf IS NOT NULL
            ORDER BY UF
        """
        df = self._qe.sql(query)
        return df["UF"].tolist() if not df.empty else []

    def get_conglomerate_members(self, cod_congl: str, start: str) -> pd.DataFrame:
        """
        Retorna membros de um conglomerado prudencial.

        Raises:
            MissingRequiredParameterError: Se start nao fornecido.
        """
        if start is None:
            raise MissingRequiredParameterError("start")

        data = self._normalize_dates(start)[0]

        conditions = [
            self._build_string_condition(
                self._storage_col("COD_CONGL_PRUD"), [cod_congl]
            ),
            self._build_int_condition(self._storage_col("DATA"), [data]),
        ]

        df = self._qe.read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            where=self._join_conditions(conditions),
        )
        return self._finalize_read(df)
