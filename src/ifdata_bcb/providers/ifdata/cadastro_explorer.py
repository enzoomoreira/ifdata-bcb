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

    _DROP_COLUMNS = ["CodInst"]

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
        query_engine: QueryEngine | None = None,
        entity_lookup: EntityLookup | None = None,
    ):
        super().__init__(query_engine, entity_lookup)
        self._collector: IFDATACadastroCollector | None = None

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

    def _build_real_entity_condition(self) -> str:
        return self._resolver.real_entity_condition()

    def _resolve_start(self, start: str | None) -> str:
        """Resolve start: se None, usa ultimo periodo disponivel."""
        if start is not None:
            return start
        latest = self._get_latest_period()
        if latest is None:
            raise MissingRequiredParameterError("start (nenhum dado disponivel)")
        return str(latest)

    def _finalize_read(self, df: pd.DataFrame) -> pd.DataFrame:
        drop_cols = [c for c in self._DROP_COLUMNS if c in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)
        return super()._finalize_read(df)

    def collect(
        self, start: str, end: str, force: bool = False, verbose: bool = True
    ) -> None:
        """Coleta dados cadastrais IFDATA do BCB (trimestral)."""
        self._get_collector().collect(start, end, force=force, verbose=verbose)

    def read(
        self,
        instituicao: InstitutionInput | None = None,
        start: str | None = None,
        end: str | None = None,
        segmento: str | None = None,
        uf: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Le dados cadastrais com filtros. Se start=None, usa ultimo periodo."""
        start = self._resolve_start(start)
        self._logger.debug(
            f"Cadastro read: instituicao={instituicao}, segmento={segmento}"
        )

        conditions = [
            self._build_cnpj_condition(instituicao),
            self._build_date_condition(start, end, trimestral=True),
            self._build_real_entity_condition(),
        ]

        if segmento:
            conditions.append(
                self._build_string_condition(
                    self._storage_col("SEGMENTO"),
                    [segmento],
                    case_insensitive=True,
                    accent_insensitive=True,
                )
            )

        if uf:
            conditions.append(
                self._build_string_condition(
                    self._storage_col("UF"),
                    [uf],
                    case_insensitive=True,
                    accent_insensitive=True,
                )
            )

        df = self._qe.read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            columns=self._translate_columns(columns),
            where=self._join_conditions(conditions),
        )
        return self._finalize_read(df)

    def info(self, instituicao: str, start: str | None = None) -> dict | None:
        """
        Retorna dict com info da instituicao no periodo especificado.
        Se start=None, usa ultimo periodo. Retorna None se nao encontrar.
        """
        start = self._resolve_start(start)
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
        if not self._qe.has_glob(self._get_pattern(), self._get_subdir()):
            return []

        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()
        query = f"""
            SELECT DISTINCT SegmentoTb as SEGMENTO
            FROM '{path}'
            WHERE SegmentoTb IS NOT NULL
              AND {self._build_real_entity_condition()}
            ORDER BY SEGMENTO
        """
        df = self._qe.sql(query)
        return df["SEGMENTO"].tolist() if not df.empty else []

    def list_ufs(self) -> list[str]:
        """Lista UFs disponiveis."""
        if not self._qe.has_glob(self._get_pattern(), self._get_subdir()):
            return []

        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()
        query = f"""
            SELECT DISTINCT Uf as UF
            FROM '{path}'
            WHERE Uf IS NOT NULL
              AND {self._build_real_entity_condition()}
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
        start = self._resolve_start(start)

        data = self._align_to_quarter_end(self._normalize_dates(start)[0])

        conditions = [
            self._build_string_condition(
                self._storage_col("COD_CONGL_PRUD"), [cod_congl]
            ),
            self._build_int_condition(self._storage_col("DATA"), [data]),
            self._build_real_entity_condition(),
        ]

        df = self._qe.read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            where=self._join_conditions(conditions),
        )
        return self._finalize_read(df)
