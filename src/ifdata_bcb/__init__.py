"""
ifdata-bcb - Analise de dados financeiros do Banco Central do Brasil.

Biblioteca para coleta e exploracao de dados bancarios do Brasil:
- COSIF: Plano Contabil das Instituicoes do Sistema Financeiro Nacional
- IFDATA: Informacoes Financeiras Trimestrais

Uso:
    import ifdata_bcb as bcb

    # Coleta de dados
    bcb.cosif.collect('2024-01', '2024-12')
    bcb.ifdata.collect('2024-01', '2024-12')

    # Buscar instituicao
    bcb.search('Itau')  # Retorna DataFrame com CNPJ_8, INSTITUICAO, FONTES, SCORE

    # Consultas usando CNPJ de 8 digitos
    # start sozinho = data unica; start + end = range de datas
    # instituicao e start sao OBRIGATORIOS
    df = bcb.ifdata.read(
        instituicao='60872504',
        start='2024-12',  # Data unica
        conta='Lucro Liquido',
    )

    df = bcb.cosif.read(
        instituicao=['60872504', '60746948'],
        start='2024-01',
        end='2024-12',  # Range de datas
        conta=['TOTAL GERAL DO ATIVO', 'PATRIMONIO LIQUIDO'],
    )  # escopo=None busca em todos os escopos
"""

# Exceptions (sempre disponiveis)
from ifdata_bcb.domain.exceptions import (
    AmbiguousIdentifierError,
    BacenAnalysisError,
    DataUnavailableError,
    EntityNotFoundError,
    InvalidDateRangeError,
    InvalidIdentifierError,
    InvalidScopeError,
    MissingRequiredParameterError,
)

# Utils
from ifdata_bcb.utils.cnpj import standardize_cnpj_base8

# Infra (para uso avancado)
from ifdata_bcb.infra import QueryEngine, DataManager

# Lazy loading dos explorers e searcher
_cosif = None
_ifdata = None
_cadastro = None
_searcher = None


def __getattr__(name):
    """Lazy loading dos explorers."""
    global _cosif, _ifdata, _cadastro

    if name == "cosif":
        if _cosif is None:
            from ifdata_bcb.providers.cosif.explorer import COSIFExplorer

            _cosif = COSIFExplorer()
        return _cosif

    if name == "ifdata":
        if _ifdata is None:
            from ifdata_bcb.providers.ifdata.explorer import IFDATAExplorer

            _ifdata = IFDATAExplorer()
        return _ifdata

    if name == "cadastro":
        if _cadastro is None:
            from ifdata_bcb.providers.cadastro.explorer import CadastroExplorer

            _cadastro = CadastroExplorer()
        return _cadastro

    raise AttributeError(f"module 'ifdata_bcb' has no attribute '{name}'")


def search(termo: str, limit: int = 10):
    """
    Busca instituicoes por nome em todas as fontes de dados.

    Use esta funcao para encontrar o CNPJ de 8 digitos de uma instituicao
    antes de fazer consultas com bcb.cosif.read(), bcb.ifdata.read(), etc.

    Args:
        termo: Nome da instituicao (parcial ou completo).
        limit: Numero maximo de resultados (padrao: 10).

    Returns:
        DataFrame com colunas:
        - CNPJ_8: CNPJ de 8 digitos (usar este valor nas consultas)
        - INSTITUICAO: Nome completo da instituicao
        - FONTES: Fontes onde a instituicao aparece (cadastro, cosif_ind, cosif_prud)
        - SCORE: Score de similaridade (0-100)

    Exemplo:
        >>> import ifdata_bcb as bcb

        >>> bcb.search('Itau Unibanco')
           CNPJ_8              INSTITUICAO                          FONTES  SCORE
        0  60872504  ITAU UNIBANCO HOLDING S.A.  cadastro,cosif_ind,cosif_prud    100

        >>> bcb.cosif.read(instituicao='60872504', start='2024-12', conta='TOTAL GERAL DO ATIVO')
    """
    global _searcher
    if _searcher is None:
        from ifdata_bcb.services.entity_searcher import EntitySearcher

        _searcher = EntitySearcher()
    return _searcher.search(termo, limit=limit)


def sql(query: str):
    """
    Executa SQL arbitrario nos dados coletados.

    Variaveis disponiveis no SQL:
        {cache} - Caminho para diretorio de cache (py-bacen/cache/)
        {raw}   - Alias para {cache} (compatibilidade)

    Args:
        query: Query SQL com placeholders opcionais.

    Returns:
        DataFrame com resultado.

    Exemplo:
        df = bcb.sql('''
            SELECT CNPJ_8, SUM(VALOR) as total
            FROM '{cache}/ifdata/valores/*.parquet'
            GROUP BY CNPJ_8
            ORDER BY total DESC
        ''')
    """
    from ifdata_bcb.infra.query import QueryEngine

    return QueryEngine().sql(query)


__all__ = [
    # Explorers (lazy)
    "cosif",
    "ifdata",
    "cadastro",
    # Funcoes de alto nivel
    "search",
    "sql",
    # Infra
    "QueryEngine",
    "DataManager",
    # Utils
    "standardize_cnpj_base8",
    # Exceptions
    "AmbiguousIdentifierError",
    "BacenAnalysisError",
    "DataUnavailableError",
    "EntityNotFoundError",
    "InvalidDateRangeError",
    "InvalidIdentifierError",
    "InvalidScopeError",
    "MissingRequiredParameterError",
]
