"""Explorer para dados cadastrais IFDATA (trimestrais)."""

from __future__ import annotations

import pandas as pd

from ifdata_bcb.core.constants import DATA_SOURCES, get_subdir
from ifdata_bcb.core.entity import EntityLookup, EntitySearch
from ifdata_bcb.domain.types import InstitutionInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import (
    build_string_condition,
    join_conditions,
)
from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.providers.ifdata.cadastro.collector import IFDATACadastroCollector
from ifdata_bcb.providers.ifdata.cadastro.search import CadastroSearch


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

    _LIST_COLUMNS: dict[str, str] = {
        "DATA": "Data",
        "SEGMENTO": "SegmentoTb",
        "UF": "Uf",
        "SITUACAO": "Situacao",
        "ATIVIDADE": "Atividade",
        "TCB": "Tcb",
        "TD": "Td",
        "TC": "Tc",
        "SR": "Sr",
        "MUNICIPIO": "Municipio",
    }

    _BLOCKED_COLUMNS: dict[str, str] = {
        "CNPJ_8": "Use cadastro.search() para buscar instituicoes.",
        "INSTITUICAO": "Use cadastro.search() para buscar instituicoes.",
    }

    def __init__(
        self,
        query_engine: QueryEngine | None = None,
        entity_lookup: EntityLookup | None = None,
    ):
        super().__init__(query_engine, entity_lookup)
        self._collector: IFDATACadastroCollector | None = None
        self._cadastro_search = CadastroSearch(
            query_engine=self._qe,
            entity_lookup=self._resolver,
            entity_search=EntitySearch(self._resolver),
        )

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

    def list(
        self,
        columns: list[str],
        *,
        start: str | None = None,
        end: str | None = None,
        segmento: str | None = None,
        uf: str | None = None,
        situacao: str | None = None,
        atividade: str | None = None,
        tcb: str | None = None,
        td: str | None = None,
        tc: str | int | None = None,
        sr: str | None = None,
        municipio: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Lista valores distintos para as colunas solicitadas.

        Args:
            columns: Colunas a listar (DATA, SEGMENTO, UF, SITUACAO, ATIVIDADE,
                     TCB, TD, TC, SR, MUNICIPIO).
            start: Periodo inicial (opcional).
            end: Periodo final (opcional).
            segmento: Filtro por segmento (case/accent insensitive).
            uf: Filtro por UF (case/accent insensitive).
            situacao: Filtro por situacao (case/accent insensitive).
            atividade: Filtro por atividade (case/accent insensitive).
            tcb: Filtro por TCB (case/accent insensitive).
            td: Filtro por TD (case/accent insensitive).
            tc: Filtro por TC (aceita str ou int).
            sr: Filtro por SR (case/accent insensitive).
            municipio: Filtro por municipio (case/accent insensitive).
            limit: Maximo de resultados.

        Raises:
            InvalidColumnError: Se coluna invalida.
        """
        return self._base_list(
            columns,
            start=start,
            end=end,
            limit=limit,
            segmento=segmento,
            uf=uf,
            situacao=situacao,
            atividade=atividade,
            tcb=tcb,
            td=td,
            tc=tc,
            sr=sr,
            municipio=municipio,
        )

    def _build_list_conditions(
        self,
        start: str | None = None,
        end: str | None = None,
        **filters: object,
    ) -> list[str | None]:
        conditions: list[str | None] = []

        # Date filter (trimestral)
        conditions.append(self._build_date_condition(start, end, trimestral=True))

        # Real entity filter (exclude alias rows)
        conditions.append(self._build_real_entidade_condition())

        # Categorical filters -- same pattern as read()
        filter_map: dict[str, str] = {
            "segmento": "SegmentoTb",
            "uf": "Uf",
            "situacao": "Situacao",
            "atividade": "Atividade",
            "tcb": "Tcb",
            "td": "Td",
            "tc": "Tc",
            "sr": "Sr",
            "municipio": "Municipio",
        }

        for param_name, storage_col in filter_map.items():
            value = filters.get(param_name)
            if value is not None:
                conditions.append(
                    build_string_condition(
                        storage_col,
                        [str(value)],
                        case_insensitive=True,
                        accent_insensitive=True,
                    )
                )

        return conditions

    def search(
        self,
        termo: str | None = None,
        *,
        fonte: str | None = None,
        escopo: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Busca instituicoes por nome ou lista todas com dados disponiveis.

        Args:
            termo: Termo de busca (fuzzy matching). Se None, lista todas.
            fonte: Filtra por fonte de dados ("ifdata", "cosif", ou None=todas).
            escopo: Filtra por escopo disponivel na fonte.
            start: Periodo inicial para verificacao de disponibilidade.
            end: Periodo final para verificacao de disponibilidade.
            limit: Maximo de resultados.

        Raises:
            InvalidScopeError: Se fonte ou escopo invalidos.
        """
        return self._cadastro_search.search(
            termo, fonte=fonte, escopo=escopo, start=start, end=end, limit=limit
        )
