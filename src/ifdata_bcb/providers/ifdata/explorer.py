"""
Explorer para dados IFDATA do BCB.

Combina coleta (IFDATAValoresCollector, IFDATACadastroCollector) e
consulta (QueryEngine) de dados IFDATA.
"""

from typing import Literal, Optional

import pandas as pd

from ifdata_bcb.services.entity_resolver import EntityResolver, ScopeResolution
from ifdata_bcb.domain.explorers import (
    AccountInput,
    BaseExplorer,
    DateInput,
    InstitutionInput,
)
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.ifdata.collector import (
    IFDATACadastroCollector,
    IFDATAValoresCollector,
)


# Tipo para o parametro escopo
EscopoIFDATA = Literal["individual", "prudencial", "financeiro"]


class IFDATAExplorer(BaseExplorer):
    """
    Explorer para dados IFDATA (Valores).

    IFDATA (Informacoes Financeiras Trimestrais) contem dados
    financeiros trimestrais das instituicoes financeiras.

    Tipos de dados suportados via IFDATAExplorer:
    - Valores financeiros (balanco, demonstracoes, etc.)

    Para dados cadastrais, use CadastroExplorer.

    Exemplo:
        explorer = IFDATAExplorer()

        # Coletar dados
        explorer.collect('2024-01', '2024-12')

        # Consultar dados (use CNPJ de 8 digitos)
        df = explorer.read(instituicoes='60872504', contas=['Lucro Líquido'], start='2024-12')
    """

    # Mapeamento de colunas: storage -> apresentacao
    _COLUMN_MAP = {
        "AnoMes": "DATA",
        "CodInst": "COD_INST",
        "TipoInstituicao": "TIPO_INST",
        "Conta": "COD_CONTA",
        "NomeColuna": "CONTA",
        "Saldo": "VALOR",
        "NomeRelatorio": "RELATORIO",
        "Grupo": "GRUPO",
    }

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        entity_resolver: Optional[EntityResolver] = None,
    ):
        """
        Inicializa o Explorer IFDATA.

        Args:
            query_engine: QueryEngine customizado. Se None, cria um novo.
            entity_resolver: EntityResolver customizado. Se None, cria um novo.
        """
        super().__init__(query_engine, entity_resolver)
        self._collector: Optional[IFDATAValoresCollector] = None

    def _get_subdir(self) -> str:
        return "ifdata/valores"

    def _get_file_prefix(self) -> str:
        return "ifdata_val"

    def _get_collector(self) -> IFDATAValoresCollector:
        """Retorna o collector (lazy initialization)."""
        if self._collector is None:
            self._collector = IFDATAValoresCollector()
        return self._collector

    def collect(
        self,
        start: str,
        end: str,
        force: bool = False,
    ) -> None:
        """
        Coleta dados IFDATA Valores do BCB.

        IFDATA Valores e trimestral, entao apenas meses de fechamento
        trimestral (03, 06, 09, 12) serao coletados.
        Estatisticas sao exibidas no banner de conclusao.

        Args:
            start: Data inicial (formato YYYY-MM).
            end: Data final (formato YYYY-MM).
            force: Se True, recoleta mesmo se dados ja existem.

        Exemplo:
            explorer.collect('2024-01', '2024-12')
        """
        collector = self._get_collector()
        collector.collect(start, end, force=force)

    def _resolve_institutions_with_scope(
        self,
        instituicoes: InstitutionInput,
        escopo: str,
    ) -> list[ScopeResolution]:
        """
        Resolve lista de instituicoes para codigos IFDATA baseado no escopo.

        Args:
            instituicoes: CNPJ(s) de 8 digitos.
            escopo: 'individual', 'prudencial', ou 'financeiro'.

        Returns:
            Lista de ScopeResolution com cod_inst e tipo_inst para cada instituicao.
        """
        if isinstance(instituicoes, str):
            instituicoes = [instituicoes]

        resolutions = []
        for inst in instituicoes:
            cnpj = self._resolve_entity(inst)
            res = self._resolver.resolve_ifdata_scope(cnpj, escopo)
            resolutions.append(res)

        return resolutions

    def _reorder_ifdata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reordena colunas do DataFrame IFDATA.

        Ordem padrao: DATA, CNPJ_8, INSTITUICAO, ESCOPO, [especificas], VALOR, [metadata]

        Args:
            df: DataFrame a reordenar.

        Returns:
            DataFrame com colunas reordenadas.
        """
        if df.empty:
            return df

        # Ordem desejada (colunas que existem)
        priority_order = [
            "DATA", "CNPJ_8", "INSTITUICAO", "ESCOPO",
            "COD_INST", "TIPO_INST", "COD_CONTA", "CONTA",
            "VALOR", "RELATORIO", "GRUPO",
        ]

        existing = [c for c in priority_order if c in df.columns]
        remaining = [c for c in df.columns if c not in existing]

        return df[existing + remaining]

    def read(
        self,
        instituicao: InstitutionInput,
        start: str,
        end: Optional[str] = None,
        conta: Optional[AccountInput] = None,
        columns: Optional[list[str]] = None,
        escopo: Optional[EscopoIFDATA] = None,
        relatorio: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Le dados IFDATA Valores com filtros.

        Args:
            instituicao: CNPJ(s) de 8 digitos. Aceita str ou lista. OBRIGATORIO.
            start: Data inicial ou unica (YYYY-MM). OBRIGATORIO.
            end: Data final (YYYY-MM). Se fornecido com start, gera range trimestral.
            conta: Nome(s) da(s) conta(s) a filtrar. Se None, retorna todas.
            columns: Colunas especificas a retornar. Se None, retorna todas.
            escopo: Escopo de dados (resolve CNPJ automaticamente):
                - 'individual': Dados da instituicao (tipo_inst=3)
                - 'prudencial': Dados do conglomerado prudencial (tipo_inst=1)
                - 'financeiro': Dados do conglomerado financeiro (tipo_inst=2)
                Se None, busca em todos os escopos disponiveis.
            relatorio: Nome do relatorio para filtrar. Use list_reports() para ver disponiveis.

        Returns:
            DataFrame com os dados filtrados. Inclui colunas:
            - DATA: Periodo (datetime)
            - CNPJ_8: CNPJ original da consulta
            - INSTITUICAO: Nome da instituicao
            - ESCOPO: Escopo dos dados (individual, prudencial, financeiro)
            - COD_INST: Codigo da instituicao (CNPJ ou codigo interno)
            - TIPO_INST: Tipo de instituicao (1=prudencial, 2=financeiro, 3=individual)
            - COD_CONTA, CONTA, VALOR, RELATORIO, GRUPO

        Raises:
            MissingRequiredParameterError: Se instituicao ou start nao fornecidos.
            InvalidDateRangeError: Se start > end.

        Exemplo:
            # Periodo unico (dezembro 2024)
            df = explorer.read(instituicao='60872504', start='2024-12')

            # Multiplas instituicoes e contas com range de datas
            df = explorer.read(
                instituicao=['60872504', '60746948'],
                conta=['Ativo Total', 'Lucro Liquido'],
                start='2024-01',
                end='2024-12'
            )

            # Dados prudenciais (Basileia) usando CNPJ individual
            df = explorer.read(
                instituicao='28195667',
                conta='Indice de Basileia',
                escopo='prudencial',
                start='2024-12'
            )
        """
        # Validar parametros obrigatorios
        self._validate_required_params(instituicao, start)

        self._logger.debug(f"IFDATA read: instituicao={instituicao}, escopo={escopo}")

        # Construir clausula WHERE
        where_parts = []

        # Mapeamento cod_inst -> lista de cnpj_8 (para adicionar coluna no resultado)
        # Usa lista pois multiplos CNPJs podem resolver para o mesmo conglomerado
        cnpj_map: dict[str, list[str]] = {}

        # Determinar escopos a buscar
        escopos_to_search = (
            [escopo] if escopo is not None
            else ["individual", "prudencial", "financeiro"]
        )

        # Resolucao com escopo(s)
        all_results = []

        for esc in escopos_to_search:
            try:
                resolutions = self._resolve_institutions_with_scope(instituicao, esc)
            except Exception:
                # Se escopo nao disponivel para alguma instituicao, pula
                continue

            # Construir mapa para adicionar CNPJ_8 no resultado
            cnpj_map_escopo: dict[str, list[str]] = {}
            for r in resolutions:
                if r.cod_inst not in cnpj_map_escopo:
                    cnpj_map_escopo[r.cod_inst] = []
                cnpj_map_escopo[r.cod_inst].append(r.cnpj_original)

            tipo_resolved = resolutions[0].tipo_inst

            codes = list(set(r.cod_inst for r in resolutions))
            where_parts_escopo = []
            # COD_INST (apresentacao) -> CodInst (storage)
            cod_inst_col = self._storage_col("COD_INST")
            if len(codes) == 1:
                where_parts_escopo.append(f"{cod_inst_col} = '{codes[0]}'")
            else:
                codes_str = ", ".join(f"'{c}'" for c in codes)
                where_parts_escopo.append(f"{cod_inst_col} IN ({codes_str})")

            # TIPO_INST (apresentacao) -> TipoInstituicao (storage)
            tipo_inst_col = self._storage_col("TIPO_INST")
            where_parts_escopo.append(f"{tipo_inst_col} = {tipo_resolved}")

            # Filtro por conta (case-insensitive)
            # CONTA (apresentacao) -> NomeColuna (storage)
            if conta:
                contas_list = self._normalize_accounts(conta)
                if contas_list:
                    where_parts_escopo.append(
                        self._build_string_condition(
                            self._storage_col("CONTA"), contas_list, case_insensitive=True
                        )
                    )

            # Filtro por datas (IFDATA e trimestral)
            # DATA (apresentacao) -> AnoMes (storage)
            datas_list = self._resolve_date_range(start, end, trimestral=True)
            if datas_list:
                data_col = self._storage_col("DATA")
                if len(datas_list) == 1:
                    where_parts_escopo.append(f"{data_col} = {datas_list[0]}")
                else:
                    datas_str = ", ".join(str(d) for d in datas_list)
                    where_parts_escopo.append(f"{data_col} IN ({datas_str})")

            # Filtro por relatorio (case-insensitive)
            # RELATORIO (apresentacao) -> NomeRelatorio (storage)
            if relatorio:
                where_parts_escopo.append(
                    self._build_string_condition(
                        self._storage_col("RELATORIO"), [relatorio], case_insensitive=True
                    )
                )

            where_clause_escopo = " AND ".join(where_parts_escopo)

            pattern = f"{self._get_file_prefix()}_*.parquet"

            df_escopo = self._qe.read_glob(
                pattern=pattern,
                subdir=self._get_subdir(),
                columns=columns,
                where=where_clause_escopo,
            )

            if not df_escopo.empty:
                df_escopo = df_escopo.copy()
                df_escopo["ESCOPO"] = esc

                # Adicionar CNPJ_8 usando merge eficiente
                # Usar nome de storage (CodInst) para o merge
                cod_inst_storage = self._storage_col("COD_INST")
                mapping_rows = []
                for cod_inst, cnpjs in cnpj_map_escopo.items():
                    for cnpj in cnpjs:
                        mapping_rows.append({cod_inst_storage: cod_inst, "CNPJ_8": cnpj})

                if mapping_rows:
                    df_mapping = pd.DataFrame(mapping_rows)
                    df_escopo = df_escopo.merge(df_mapping, on=cod_inst_storage, how="left")
                else:
                    df_escopo["CNPJ_8"] = df_escopo[cod_inst_storage]

                all_results.append(df_escopo)

        # Concatenar resultados de todos os escopos
        if not all_results:
            # Retornar DataFrame vazio com colunas esperadas
            return pd.DataFrame(columns=[
                "DATA", "CNPJ_8", "INSTITUICAO", "ESCOPO",
                "COD_INST", "TIPO_INST", "COD_CONTA", "CONTA",
                "VALOR", "RELATORIO", "GRUPO",
            ])

        df = pd.concat(all_results, ignore_index=True)

        # Adicionar INSTITUICAO
        if not df.empty and "CNPJ_8" in df.columns:
            cnpjs_unicos = df["CNPJ_8"].unique().tolist()
            nomes_map = self._resolver.get_names_for_cnpjs(cnpjs_unicos)
            df["INSTITUICAO"] = df["CNPJ_8"].map(nomes_map)

        # Reordenar colunas e finalizar
        df = self._reorder_ifdata_columns(df)
        self._logger.debug(f"IFDATA result: {len(df)} rows")
        return self._finalize_read(df)

    def read_by_account_code(
        self,
        cod_conta: str,
        instituicao: InstitutionInput,
        start: str,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Le dados por codigo de conta IFDATA.

        Args:
            cod_conta: Codigo da conta.
            instituicao: CNPJ(s) de 8 digitos. OBRIGATORIO.
            start: Data inicial ou unica (YYYY-MM). OBRIGATORIO.
            end: Data final (YYYY-MM). Se fornecido com start, gera range trimestral.

        Returns:
            DataFrame com os dados filtrados. Coluna DATA em formato datetime.

        Raises:
            MissingRequiredParameterError: Se instituicao ou start nao fornecidos.
            InvalidDateRangeError: Se start > end.
        """
        # Validar parametros obrigatorios
        self._validate_required_params(instituicao, start)

        # COD_CONTA (apresentacao) -> Conta (storage)
        cod_conta_col = self._storage_col("COD_CONTA")
        where_parts = [f"{cod_conta_col} = '{cod_conta}'"]

        # COD_INST (apresentacao) -> CodInst (storage)
        cnpjs = self._normalize_institutions(instituicao)
        if cnpjs:
            cod_inst_col = self._storage_col("COD_INST")
            if len(cnpjs) == 1:
                where_parts.append(f"{cod_inst_col} = '{cnpjs[0]}'")
            else:
                cnpjs_str = ", ".join(f"'{c}'" for c in cnpjs)
                where_parts.append(f"{cod_inst_col} IN ({cnpjs_str})")

        # IFDATA e trimestral
        # DATA (apresentacao) -> AnoMes (storage)
        datas_list = self._resolve_date_range(start, end, trimestral=True)
        if datas_list:
            data_col = self._storage_col("DATA")
            if len(datas_list) == 1:
                where_parts.append(f"{data_col} = {datas_list[0]}")
            else:
                datas_str = ", ".join(str(d) for d in datas_list)
                where_parts.append(f"{data_col} IN ({datas_str})")

        where_clause = " AND ".join(where_parts)

        pattern = f"{self._get_file_prefix()}_*.parquet"

        df = self._qe.read_glob(
            pattern=pattern,
            subdir=self._get_subdir(),
            where=where_clause,
        )
        return self._finalize_read(df)

    def list_accounts(self, limit: int = 100) -> pd.DataFrame:
        """
        Lista contas disponiveis nos dados.

        Args:
            limit: Numero maximo de contas a retornar.

        Returns:
            DataFrame com colunas COD_CONTA e CONTA.
        """
        pattern = f"{self._get_file_prefix()}_*.parquet"
        path = self._qe.cache_path / self._get_subdir() / pattern

        # Query usa nomes de storage e renomeia para apresentacao
        # Conta (storage) -> COD_CONTA (apresentacao)
        # NomeColuna (storage) -> CONTA (apresentacao)
        query = f"""
            SELECT DISTINCT
                Conta as COD_CONTA,
                NomeColuna as CONTA
            FROM '{path}'
            ORDER BY COD_CONTA
            LIMIT {limit}
        """

        return self._qe.sql(query)

    def list_institutions(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Lista instituicoes disponiveis nos dados.

        Args:
            start: Data inicial ou unica (YYYY-MM). Se None, considera todos.
            end: Data final (YYYY-MM). Se fornecido com start, gera range trimestral.

        Returns:
            DataFrame com colunas COD_INST, TIPO_INST e INSTITUICAO (para tipo_inst=3).
        """
        pattern = f"{self._get_file_prefix()}_*.parquet"
        path = self._qe.cache_path / self._get_subdir() / pattern

        # AnoMes (storage) -> DATA (apresentacao)
        where = ""
        datas_list = self._resolve_date_range(start, end, trimestral=True)
        if datas_list:
            if len(datas_list) == 1:
                where = f"WHERE AnoMes = {datas_list[0]}"
            else:
                datas_str = ", ".join(str(d) for d in datas_list)
                where = f"WHERE AnoMes IN ({datas_str})"

        # Query usa nomes de storage e renomeia para apresentacao
        query = f"""
            SELECT DISTINCT
                CodInst as COD_INST,
                TipoInstituicao as TIPO_INST
            FROM '{path}'
            {where}
            ORDER BY COD_INST
        """

        df = self._qe.sql(query)

        # Adicionar INSTITUICAO para instituicoes individuais (tipo_inst=3)
        if not df.empty:
            # Filtrar apenas tipo_inst=3 (individuais) que sao CNPJs de 8 digitos
            mask_individual = df["TIPO_INST"] == 3
            cnpjs_individuais = df.loc[mask_individual, "COD_INST"].tolist()

            if cnpjs_individuais:
                nomes_map = self._resolver.get_names_for_cnpjs(cnpjs_individuais)
                df["INSTITUICAO"] = df.apply(
                    lambda row: nomes_map.get(row["COD_INST"], "") if row["TIPO_INST"] == 3 else "",
                    axis=1
                )
            else:
                df["INSTITUICAO"] = ""

        return df

    def list_reports(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> list[str]:
        """
        Lista relatorios disponiveis nos dados IFDATA.

        Args:
            start: Data inicial ou unica (YYYY-MM). Se None, busca em todos.
            end: Data final (YYYY-MM). Se fornecido com start, gera range trimestral.

        Returns:
            Lista de nomes de relatorios unicos, ordenados alfabeticamente.

        Exemplo:
            >>> explorer.list_reports()
            ['Ativo', 'Carteira de credito ativa...', 'Demonstracao de Resultado', ...]
        """
        pattern = f"{self._get_file_prefix()}_*.parquet"
        path = self._qe.cache_path / self._get_subdir() / pattern

        # AnoMes (storage) -> DATA (apresentacao)
        where = ""
        datas_list = self._resolve_date_range(start, end, trimestral=True)
        if datas_list:
            if len(datas_list) == 1:
                where = f"WHERE AnoMes = {datas_list[0]}"
            else:
                datas_str = ", ".join(str(d) for d in datas_list)
                where = f"WHERE AnoMes IN ({datas_str})"

        # Query usa nome de storage e renomeia para apresentacao
        # NomeRelatorio (storage) -> RELATORIO (apresentacao)
        query = f"""
            SELECT DISTINCT NomeRelatorio as RELATORIO
            FROM '{path}'
            {where}
            ORDER BY RELATORIO
        """

        df = self._qe.sql(query)
        return df["RELATORIO"].tolist() if not df.empty else []


class CadastroExplorer(BaseExplorer):
    """
    Explorer para dados cadastrais IFDATA.

    Contem informacoes cadastrais das instituicoes financeiras:
    - Nome
    - Segmento
    - Conglomerado prudencial/financeiro
    - Instituicao lider
    - Situacao, UF, etc.

    Exemplo:
        explorer = CadastroExplorer()

        # Coletar dados
        explorer.collect('2024-01', '2024-12')

        # Consultar dados de uma instituicao
        df = explorer.read('60872504')

        # Buscar todas instituicoes de um segmento
        df = explorer.read(segmento='Banco Múltiplo')
    """

    # Mapeamento de colunas: storage -> apresentacao
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
        # CNPJ_8, CNPJ_LIDER_8: sem mudanca
    }

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        entity_resolver: Optional[EntityResolver] = None,
    ):
        """
        Inicializa o Explorer de Cadastro.

        Args:
            query_engine: QueryEngine customizado. Se None, cria um novo.
            entity_resolver: EntityResolver customizado. Se None, cria um novo.
        """
        super().__init__(query_engine, entity_resolver)
        self._collector: Optional[IFDATACadastroCollector] = None

    def _get_subdir(self) -> str:
        return "ifdata/cadastro"

    def _get_file_prefix(self) -> str:
        return "ifdata_cad"

    def _get_collector(self) -> IFDATACadastroCollector:
        """Retorna o collector (lazy initialization)."""
        if self._collector is None:
            self._collector = IFDATACadastroCollector()
        return self._collector

    def collect(
        self,
        start: str,
        end: str,
        force: bool = False,
    ) -> None:
        """
        Coleta dados IFDATA Cadastro do BCB.

        Estatisticas sao exibidas no banner de conclusao.

        Args:
            start: Data inicial (formato YYYY-MM).
            end: Data final (formato YYYY-MM).
            force: Se True, recoleta mesmo se dados ja existem.
        """
        collector = self._get_collector()
        collector.collect(start, end, force=force)

    def read(
        self,
        instituicao: Optional[InstitutionInput] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        segmento: Optional[str] = None,
        uf: Optional[str] = None,
        columns: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        Le dados cadastrais com filtros opcionais.

        Args:
            instituicao: CNPJ(s) de 8 digitos. Aceita str ou lista. Se None, retorna todas.
            start: Data inicial ou unica (YYYY-MM). Se end=None, filtra apenas este periodo.
            end: Data final (YYYY-MM). Se fornecido com start, gera range trimestral.
            segmento: Segmento para filtrar (ex: 'Banco Multiplo', 'Cooperativa de Credito').
                     Use list_segmentos() para ver opcoes disponiveis. Case-insensitive.
            uf: UF para filtrar (ex: 'SP', 'RJ'). Case-insensitive.
            columns: Colunas especificas a retornar. Se None, retorna todas.

        Returns:
            DataFrame com os dados filtrados. Coluna DATA em formato datetime.

        Colunas disponiveis:
        - DATA: Periodo (datetime)
        - CNPJ_8: CNPJ de 8 digitos
        - INSTITUICAO: Nome da instituicao
        - SEGMENTO: Segmento (B1, B2, S1, etc.)
        - COD_CONGL_PRUD: Codigo do conglomerado prudencial
        - COD_CONGL_FIN: Codigo do conglomerado financeiro
        - CNPJ_LIDER_8: CNPJ do lider do conglomerado
        - SITUACAO: Situacao da instituicao (A=Ativo)
        - ATIVIDADE: Atividade principal
        - TCB, TD, TC: Classificacoes regulatorias
        - UF, MUNICIPIO, SR: Localizacao
        - DATA_INICIO_ATIVIDADE: Data de inicio das atividades

        Exemplo:
            # Dados de uma instituicao (use CNPJ de 8 digitos)
            df = explorer.read(instituicao='60872504')

            # Periodo unico
            df = explorer.read(instituicao='60872504', start='2024-12')

            # Multiplas instituicoes com range de datas
            df = explorer.read(
                instituicao=['60872504', '60746948'],
                start='2024-01',
                end='2024-12'
            )

            # Todas instituicoes de um segmento
            df = explorer.read(segmento='Banco Multiplo')

            # Instituicoes de SP
            df = explorer.read(uf='SP')
        """
        self._logger.debug(f"Cadastro read: instituicao={instituicao}, segmento={segmento}")
        where_parts = []

        # CNPJ_8 nao muda (ja e storage)
        cnpjs = self._normalize_institutions(instituicao)
        if cnpjs:
            if len(cnpjs) == 1:
                where_parts.append(f"CNPJ_8 = '{cnpjs[0]}'")
            else:
                cnpjs_str = ", ".join(f"'{c}'" for c in cnpjs)
                where_parts.append(f"CNPJ_8 IN ({cnpjs_str})")

        # Cadastro e trimestral (mesmo que IFDATA)
        # DATA (apresentacao) -> Data (storage)
        datas_list = self._resolve_date_range(start, end, trimestral=True)
        if datas_list:
            data_col = self._storage_col("DATA")
            if len(datas_list) == 1:
                where_parts.append(f"{data_col} = {datas_list[0]}")
            else:
                datas_str = ", ".join(str(d) for d in datas_list)
                where_parts.append(f"{data_col} IN ({datas_str})")

        # Filtros case-insensitive usando _build_string_condition
        # SEGMENTO (apresentacao) -> SegmentoTb (storage)
        if segmento:
            where_parts.append(
                self._build_string_condition(
                    self._storage_col("SEGMENTO"), [segmento], case_insensitive=True
                )
            )

        # UF (apresentacao) -> Uf (storage)
        if uf:
            where_parts.append(
                self._build_string_condition(
                    self._storage_col("UF"), [uf], case_insensitive=True
                )
            )

        where_clause = " AND ".join(where_parts) if where_parts else None

        pattern = f"{self._get_file_prefix()}_*.parquet"

        df = self._qe.read_glob(
            pattern=pattern,
            subdir=self._get_subdir(),
            columns=columns,
            where=where_clause,
        )
        return self._finalize_read(df)

    def info(
        self,
        instituicao: str,
        start: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Retorna informacoes detalhadas de uma instituicao.

        Args:
            instituicao: CNPJ de 8 digitos da instituicao.
            start: Periodo especifico (YYYY-MM). Se None, retorna o mais recente.

        Returns:
            Dicionario com informacoes da instituicao ou None se nao encontrar.
            Valores "null" sao convertidos para None.

        Exemplo:
            >>> bcb.cadastro.info('60872504')  # Mais recente
            {'DATA': datetime(2024, 12, 1), 'CNPJ_8': '60872504', ...}

            >>> bcb.cadastro.info('60872504', start='2024-03')  # Periodo especifico
        """
        cnpj = self._resolve_entity(instituicao)

        # Se start nao fornecido, buscar todos os periodos para pegar o mais recente
        df = self.read(instituicao=cnpj, start=start)

        if df.empty:
            self._logger.warning(f"Institution not found: {instituicao}")
            return None

        # Pegar registro mais recente
        df_sorted = df.sort_values(by="DATA", ascending=False)
        row = df_sorted.iloc[0]

        # Converter para dict e substituir "null" strings por None
        result = row.to_dict()
        for key, value in result.items():
            if value == "null":
                result[key] = None

        return result

    def list_segmentos(self) -> list[str]:
        """
        Lista segmentos disponiveis.

        Returns:
            Lista de segmentos unicos.
        """
        pattern = f"{self._get_file_prefix()}_*.parquet"
        path = self._qe.cache_path / self._get_subdir() / pattern

        # SegmentoTb (storage) -> SEGMENTO (apresentacao)
        query = f"""
            SELECT DISTINCT SegmentoTb as SEGMENTO
            FROM '{path}'
            WHERE SegmentoTb IS NOT NULL
            ORDER BY SEGMENTO
        """

        df = self._qe.sql(query)
        return df["SEGMENTO"].tolist() if not df.empty else []

    def list_ufs(self) -> list[str]:
        """
        Lista UFs disponiveis.

        Returns:
            Lista de UFs unicos.
        """
        pattern = f"{self._get_file_prefix()}_*.parquet"
        path = self._qe.cache_path / self._get_subdir() / pattern

        # Uf (storage) -> UF (apresentacao)
        query = f"""
            SELECT DISTINCT Uf as UF
            FROM '{path}'
            WHERE Uf IS NOT NULL
            ORDER BY UF
        """

        df = self._qe.sql(query)
        return df["UF"].tolist() if not df.empty else []

    def get_conglomerate_members(
        self,
        cod_congl: str,
        start: str,
    ) -> pd.DataFrame:
        """
        Retorna membros de um conglomerado prudencial.

        Args:
            cod_congl: Codigo do conglomerado prudencial.
            start: Periodo especifico (YYYY-MM). OBRIGATORIO.

        Returns:
            DataFrame com membros do conglomerado.

        Raises:
            MissingRequiredParameterError: Se start nao fornecido.
        """
        from ifdata_bcb.domain.exceptions import MissingRequiredParameterError

        if start is None:
            raise MissingRequiredParameterError(
                "start",
                "Especifique o periodo (formato YYYY-MM).",
            )

        # Normalizar data para formato YYYYMM
        data_normalized = self._normalize_dates(start)[0]

        # Usar nomes de storage
        # COD_CONGL_PRUD (apresentacao) -> CodConglomeradoPrudencial (storage)
        # DATA (apresentacao) -> Data (storage)
        cod_congl_col = self._storage_col("COD_CONGL_PRUD")
        data_col = self._storage_col("DATA")
        where_parts = [
            f"{cod_congl_col} = '{cod_congl}'",
            f"{data_col} = {data_normalized}",
        ]

        where_clause = " AND ".join(where_parts)

        pattern = f"{self._get_file_prefix()}_*.parquet"

        df = self._qe.read_glob(
            pattern=pattern,
            subdir=self._get_subdir(),
            where=where_clause,
        )
        return self._finalize_read(df)
