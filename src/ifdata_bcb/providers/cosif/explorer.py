"""
Explorer para dados COSIF do BCB.

Combina coleta (COSIFCollector) e consulta (QueryEngine) de dados COSIF.
"""

from typing import Literal, Optional

import pandas as pd

from ifdata_bcb.domain.explorers import (
    AccountInput,
    BaseExplorer,
    DateInput,
    InstitutionInput,
)
from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.cosif.collector import COSIFCollector
from ifdata_bcb.services.entity_resolver import EntityResolver
from ifdata_bcb.ui.display import get_display

# Tipo para escopo COSIF
EscopoCOSIF = Literal["individual", "prudencial"]


class COSIFExplorer(BaseExplorer):
    """
    Explorer para dados COSIF.

    COSIF (Plano Contabil das Instituicoes do Sistema Financeiro Nacional)
    contem dados contabeis mensais das instituicoes financeiras.

    Escopos suportados:
    - 'individual': Dados de instituicoes individuais
    - 'prudencial': Dados de conglomerados prudenciais

    IMPORTANTE:
    - read() e read_by_account_code() EXIGEM escopo explicito
    - list_accounts() e list_institutions() retornam ambos escopos se None
    - Coluna DATA retorna datetime (nao int YYYYMM)

    Exemplo:
        import ifdata_bcb as bcb

        # Coletar dados (ambos escopos por padrao)
        bcb.cosif.collect('2024-01', '2024-12')

        # Coletar apenas um escopo
        bcb.cosif.collect('2024-01', '2024-12', escopo='individual')

        # Consultar dados (escopo OBRIGATORIO)
        df = bcb.cosif.read('60872504', contas=['TOTAL GERAL DO ATIVO'],
                           datas=202412, escopo='prudencial')

        # Listar instituicoes (sem escopo = ambos)
        df = bcb.cosif.list_institutions(datas=202412)  # Inclui coluna ESCOPO
    """

    # Mapeamento de colunas: storage -> apresentacao
    _COLUMN_MAP = {
        "DATA_BASE": "DATA",
        "NOME_INSTITUICAO": "INSTITUICAO",
        "CONTA": "COD_CONTA",
        "NOME_CONTA": "CONTA",
        "SALDO": "VALOR",
        # CNPJ_8, DOCUMENTO: sem mudanca
    }

    _ESCOPOS: dict[str, dict[str, str]] = {
        "individual": {
            "subdir": "cosif/individual",
            "prefix": "cosif_ind",
        },
        "prudencial": {
            "subdir": "cosif/prudencial",
            "prefix": "cosif_prud",
        },
    }

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        entity_resolver: Optional[EntityResolver] = None,
    ):
        """
        Inicializa o Explorer COSIF.

        Args:
            query_engine: QueryEngine customizado. Se None, cria um novo.
            entity_resolver: EntityResolver customizado. Se None, cria um novo.
        """
        super().__init__(query_engine, entity_resolver)

    def _get_subdir(self, escopo: EscopoCOSIF) -> str:
        """Retorna subdiretorio para o escopo."""
        return self._ESCOPOS[escopo]["subdir"]

    def _get_file_prefix(self, escopo: EscopoCOSIF) -> str:
        """Retorna prefixo de arquivo para o escopo."""
        return self._ESCOPOS[escopo]["prefix"]

    def _validate_escopo(self, escopo: str) -> EscopoCOSIF:
        """Valida e normaliza o escopo."""
        escopo_lower = escopo.lower()
        if escopo_lower not in self._ESCOPOS:
            self._logger.warning(f"Invalid escopo: {escopo}")
            valid = ", ".join(self._ESCOPOS.keys())
            raise ValueError(f"Escopo '{escopo}' invalido. Validos: {valid}")
        return escopo_lower  # type: ignore

    def _reorder_cosif_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reordena colunas do DataFrame COSIF.

        Ordem padrao: DATA, CNPJ_8, INSTITUICAO, ESCOPO, DOCUMENTO, COD_CONTA, CONTA, VALOR

        Args:
            df: DataFrame a reordenar.

        Returns:
            DataFrame com colunas reordenadas.
        """
        if df.empty:
            return df

        priority_order = [
            "DATA", "CNPJ_8", "INSTITUICAO", "ESCOPO",
            "DOCUMENTO", "COD_CONTA", "CONTA", "VALOR",
        ]

        existing = [c for c in priority_order if c in df.columns]
        remaining = [c for c in df.columns if c not in existing]

        return df[existing + remaining]

    def collect(
        self,
        start: str,
        end: str,
        escopo: Optional[EscopoCOSIF] = None,
        force: bool = False,
        verbose: bool = True,
    ) -> None:
        """
        Coleta dados COSIF do BCB.

        Quando coleta ambos os escopos (padrao), exibe progresso unificado.
        Estatisticas consolidadas sao exibidas no banner de conclusao.

        Args:
            start: Data inicial (formato YYYY-MM).
            end: Data final (formato YYYY-MM).
            escopo: 'individual', 'prudencial', ou None para ambos (padrao).
            force: Se True, recoleta mesmo se dados ja existem.
            verbose: Se True, exibe banners e progresso.

        Exemplo:
            # Coletar ambos os escopos
            bcb.cosif.collect('2024-01', '2024-12')

            # Coletar apenas prudencial
            bcb.cosif.collect('2024-01', '2024-12', escopo='prudencial')
        """
        if escopo is not None:
            # Escopo unico: delega diretamente ao collector
            escopo = self._validate_escopo(escopo)
            collector = COSIFCollector(escopo)
            collector.collect(start, end, force=force, verbose=verbose)
        else:
            # Ambos escopos: controle unificado do display
            self._collect_all_escopos(start, end, force=force, verbose=verbose)

    def _collect_all_escopos(
        self,
        start: str,
        end: str,
        force: bool = False,
        verbose: bool = True,
    ) -> None:
        """
        Coleta todos os escopos com display unificado.

        Orquestra os collectors, controlando banners externamente.

        Args:
            start: Data inicial.
            end: Data final.
            force: Se True, recoleta.
            verbose: Se True, exibe progresso.
        """
        display = get_display(verbose)

        # Criar collectors e calcular periodos faltantes
        collectors_info: list[tuple[str, COSIFCollector, int]] = []
        total_periodos = 0

        for esc in self._ESCOPOS:
            collector = COSIFCollector(esc)
            if force:
                periods = collector._generate_periods(start, end)
            else:
                periods = collector._get_missing_periods(start, end)
            if periods:
                collectors_info.append((esc, collector, len(periods)))
                total_periodos += len(periods)

        if not collectors_info:
            display.print_info("COSIF: Dados ja atualizados")
            return

        # Banner unico de inicio
        escopos_str = " + ".join(esc.capitalize() for esc, _, _ in collectors_info)
        display.banner(f"Coletando COSIF ({escopos_str})", indicator_count=total_periodos)

        # Coletar cada escopo (banners desabilitados, progresso com desc customizado)
        total_registros = 0
        total_falhas = 0
        total_indisponiveis = 0
        periodos_ok = 0

        for esc, collector, num_periods in collectors_info:
            # Coleta com banners desabilitados e descricao customizada
            registros, ok, falhas, indisponiveis = collector.collect(
                start,
                end,
                force=force,
                verbose=verbose,
                progress_desc=f"  {esc.capitalize()}",
                _show_banners=False,
            )

            total_registros += registros
            total_falhas += falhas
            total_indisponiveis += indisponiveis
            periodos_ok += ok

        # Banner unico de conclusao
        display.end_banner(
            total=total_registros if total_registros > 0 else None,
            periodos=periodos_ok,
            falhas=total_falhas if total_falhas > 0 else None,
            indisponiveis=total_indisponiveis if total_indisponiveis > 0 else None,
        )

    def read(
        self,
        instituicao: InstitutionInput,
        start: str,
        end: Optional[str] = None,
        conta: Optional[AccountInput] = None,
        escopo: Optional[EscopoCOSIF] = None,
        columns: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        Le dados COSIF com filtros.

        Args:
            instituicao: CNPJ(s) de 8 digitos. Aceita str ou lista. OBRIGATORIO.
            start: Data inicial ou unica (YYYY-MM). OBRIGATORIO.
            end: Data final (YYYY-MM). Se fornecido com start, gera range mensal.
            conta: Nome(s) da(s) conta(s) a filtrar. Se None, retorna todas.
            escopo: 'individual', 'prudencial', ou None para buscar em TODOS os escopos.
            columns: Colunas especificas a retornar. Se None, retorna todas.

        Returns:
            DataFrame com os dados filtrados. Inclui coluna ESCOPO.
            Coluna DATA em formato datetime.

        Raises:
            MissingRequiredParameterError: Se instituicao ou start nao fornecidos.
            InvalidDateRangeError: Se start > end.

        Colunas disponiveis:
        - DATA: Periodo (datetime)
        - CNPJ_8: CNPJ de 8 digitos
        - INSTITUICAO: Nome da instituicao
        - ESCOPO: Escopo dos dados (individual, prudencial)
        - DOCUMENTO: Documento (geralmente 7 ou 8)
        - COD_CONTA: Codigo da conta COSIF
        - CONTA: Nome/descricao da conta
        - VALOR: Valor em reais

        Exemplo:
            # Periodo unico com escopo especifico
            df = bcb.cosif.read(
                instituicao='60872504',
                start='2024-12',
                conta='TOTAL GERAL DO ATIVO',
                escopo='prudencial'
            )

            # Busca em todos os escopos (escopo=None)
            df = bcb.cosif.read(
                instituicao='60872504',
                start='2024-01',
                end='2024-12',
                conta=['TOTAL GERAL DO ATIVO', 'PATRIMONIO LIQUIDO']
            )
        """
        # Validar parametros obrigatorios
        self._validate_required_params(instituicao, start)

        self._logger.debug(f"COSIF read: escopo={escopo}, instituicao={instituicao}")

        # Determinar escopos a buscar
        if escopo is not None:
            escopo = self._validate_escopo(escopo)
            escopos_to_search = [escopo]
        else:
            escopos_to_search = list(self._ESCOPOS.keys())

        all_results = []

        for esc in escopos_to_search:
            df_escopo = self._read_single_escopo(
                instituicao, conta, start, end, esc, columns
            )
            if not df_escopo.empty:
                df_escopo = df_escopo.copy()
                df_escopo["ESCOPO"] = esc
                all_results.append(df_escopo)

        if not all_results:
            # Retornar DataFrame vazio com colunas esperadas
            return pd.DataFrame(columns=[
                "DATA", "CNPJ_8", "INSTITUICAO", "ESCOPO",
                "DOCUMENTO", "COD_CONTA", "CONTA", "VALOR",
            ])

        df = pd.concat(all_results, ignore_index=True)
        df = self._reorder_cosif_columns(df)
        self._logger.debug(f"COSIF result: {len(df)} rows")
        return self._finalize_read(df)

    def _read_single_escopo(
        self,
        instituicao: InstitutionInput,
        conta: Optional[AccountInput],
        start: str,
        end: Optional[str],
        escopo: EscopoCOSIF,
        columns: Optional[list[str]],
    ) -> pd.DataFrame:
        """Le dados de um escopo especifico usando nomes de storage."""
        where_parts = []

        # Filtro por instituicao (CNPJ_8 nao muda)
        cnpjs = self._normalize_institutions(instituicao)
        if cnpjs:
            if len(cnpjs) == 1:
                where_parts.append(f"CNPJ_8 = '{cnpjs[0]}'")
            else:
                cnpjs_str = ", ".join(f"'{c}'" for c in cnpjs)
                where_parts.append(f"CNPJ_8 IN ({cnpjs_str})")

        # Filtro por conta (case-insensitive) - usa nome de storage (NOME_CONTA)
        if conta:
            contas_list = self._normalize_accounts(conta)
            if contas_list:
                # CONTA (apresentacao) -> NOME_CONTA (storage)
                where_parts.append(
                    self._build_string_condition(
                        self._storage_col("CONTA"), contas_list, case_insensitive=True
                    )
                )

        # Filtro por datas (COSIF e mensal) - usa nome de storage (DATA_BASE)
        datas_list = self._resolve_date_range(start, end, trimestral=False)
        if datas_list:
            # DATA (apresentacao) -> DATA_BASE (storage)
            data_col = self._storage_col("DATA")
            if len(datas_list) == 1:
                where_parts.append(f"{data_col} = {datas_list[0]}")
            else:
                datas_str = ", ".join(str(d) for d in datas_list)
                where_parts.append(f"{data_col} IN ({datas_str})")

        where_clause = " AND ".join(where_parts) if where_parts else None

        pattern = f"{self._get_file_prefix(escopo)}_*.parquet"

        return self._qe.read_glob(
            pattern=pattern,
            subdir=self._get_subdir(escopo),
            columns=columns,
            where=where_clause,
        )

    def read_by_account_code(
        self,
        cod_conta: str,
        instituicao: InstitutionInput,
        start: str,
        end: Optional[str] = None,
        escopo: Optional[EscopoCOSIF] = None,
    ) -> pd.DataFrame:
        """
        Le dados por codigo de conta COSIF.

        Args:
            cod_conta: Codigo da conta COSIF (ex: '1.0.0.00.00-2').
            instituicao: CNPJ(s) de 8 digitos. OBRIGATORIO.
            start: Data inicial ou unica (YYYY-MM). OBRIGATORIO.
            end: Data final (YYYY-MM). Se fornecido com start, gera range mensal.
            escopo: 'individual', 'prudencial', ou None para buscar em TODOS os escopos.

        Returns:
            DataFrame com os dados filtrados. Inclui coluna ESCOPO.
            Coluna DATA em formato datetime.

        Raises:
            MissingRequiredParameterError: Se instituicao ou start nao fornecidos.
            InvalidDateRangeError: Se start > end.

        Exemplo:
            # Ativo total (cod 1.0.0.00.00-2)
            df = bcb.cosif.read_by_account_code(
                '1.0.0.00.00-2',
                instituicao='60872504',
                start='2024-12',
                escopo='prudencial'
            )
        """
        # Validar parametros obrigatorios
        self._validate_required_params(instituicao, start)

        # Determinar escopos a buscar
        if escopo is not None:
            escopo = self._validate_escopo(escopo)
            escopos_to_search = [escopo]
        else:
            escopos_to_search = list(self._ESCOPOS.keys())

        all_results = []

        for esc in escopos_to_search:
            # COD_CONTA (apresentacao) -> CONTA (storage)
            cod_conta_col = self._storage_col("COD_CONTA")
            where_parts = [f"{cod_conta_col} = '{cod_conta}'"]

            cnpjs = self._normalize_institutions(instituicao)
            if cnpjs:
                if len(cnpjs) == 1:
                    where_parts.append(f"CNPJ_8 = '{cnpjs[0]}'")
                else:
                    cnpjs_str = ", ".join(f"'{c}'" for c in cnpjs)
                    where_parts.append(f"CNPJ_8 IN ({cnpjs_str})")

            # COSIF e mensal - usa nome de storage (DATA_BASE)
            datas_list = self._resolve_date_range(start, end, trimestral=False)
            if datas_list:
                data_col = self._storage_col("DATA")
                if len(datas_list) == 1:
                    where_parts.append(f"{data_col} = {datas_list[0]}")
                else:
                    datas_str = ", ".join(str(d) for d in datas_list)
                    where_parts.append(f"{data_col} IN ({datas_str})")

            where_clause = " AND ".join(where_parts)

            pattern = f"{self._get_file_prefix(esc)}_*.parquet"

            df_escopo = self._qe.read_glob(
                pattern=pattern,
                subdir=self._get_subdir(esc),
                where=where_clause,
            )

            if not df_escopo.empty:
                df_escopo = df_escopo.copy()
                df_escopo["ESCOPO"] = esc
                all_results.append(df_escopo)

        if not all_results:
            return pd.DataFrame()

        df = pd.concat(all_results, ignore_index=True)
        df = self._reorder_cosif_columns(df)
        return self._finalize_read(df)

    def list_accounts(
        self,
        escopo: Optional[EscopoCOSIF] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Lista contas disponiveis nos dados.

        Args:
            escopo: 'individual', 'prudencial', ou None para ambos.
            limit: Numero maximo de contas a retornar (por escopo se None).

        Returns:
            DataFrame com colunas COD_CONTA, CONTA e ESCOPO (quando None).
        """
        if escopo is not None:
            escopo = self._validate_escopo(escopo)
            return self._list_accounts_single(escopo, limit)

        # Concatenar ambos escopos
        dfs = []
        for esc in self._ESCOPOS:
            df = self._list_accounts_single(esc, limit)
            df["ESCOPO"] = esc
            dfs.append(df)

        return pd.concat(dfs, ignore_index=True)

    def _list_accounts_single(
        self,
        escopo: EscopoCOSIF,
        limit: int,
    ) -> pd.DataFrame:
        """Lista contas para um escopo especifico usando nomes de storage."""
        pattern = f"{self._get_file_prefix(escopo)}_*.parquet"
        path = self._qe.cache_path / self._get_subdir(escopo) / pattern

        # Query usa nomes de storage e renomeia para apresentacao
        # CONTA (storage) -> COD_CONTA (apresentacao)
        # NOME_CONTA (storage) -> CONTA (apresentacao)
        query = f"""
            SELECT DISTINCT
                CONTA as COD_CONTA,
                NOME_CONTA as CONTA
            FROM '{path}'
            ORDER BY COD_CONTA
            LIMIT {limit}
        """

        return self._qe.sql(query)

    def list_institutions(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
        escopo: Optional[EscopoCOSIF] = None,
    ) -> pd.DataFrame:
        """
        Lista instituicoes disponiveis nos dados.

        Args:
            start: Data inicial ou unica (YYYY-MM). Se None, considera todos.
            end: Data final (YYYY-MM). Se fornecido com start, gera range mensal.
            escopo: 'individual', 'prudencial', ou None para ambos.

        Returns:
            DataFrame com colunas CNPJ_8, INSTITUICAO e ESCOPO (quando None).
        """
        if escopo is not None:
            escopo = self._validate_escopo(escopo)
            return self._list_institutions_single(escopo, start, end)

        # Concatenar ambos escopos
        dfs = []
        for esc in self._ESCOPOS:
            df = self._list_institutions_single(esc, start, end)
            df["ESCOPO"] = esc
            dfs.append(df)

        return pd.concat(dfs, ignore_index=True)

    def _list_institutions_single(
        self,
        escopo: EscopoCOSIF,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Lista instituicoes para um escopo especifico usando nomes de storage."""
        pattern = f"{self._get_file_prefix(escopo)}_*.parquet"
        path = self._qe.cache_path / self._get_subdir(escopo) / pattern

        # DATA (apresentacao) -> DATA_BASE (storage)
        where = ""
        datas_list = self._resolve_date_range(start, end, trimestral=False)
        if datas_list:
            if len(datas_list) == 1:
                where = f"WHERE DATA_BASE = {datas_list[0]}"
            else:
                datas_str = ", ".join(str(d) for d in datas_list)
                where = f"WHERE DATA_BASE IN ({datas_str})"

        # Query usa nomes de storage e renomeia para apresentacao
        # NOME_INSTITUICAO (storage) -> INSTITUICAO (apresentacao)
        query = f"""
            SELECT DISTINCT CNPJ_8, NOME_INSTITUICAO as INSTITUICAO
            FROM '{path}'
            {where}
            ORDER BY INSTITUICAO
        """

        return self._qe.sql(query)
