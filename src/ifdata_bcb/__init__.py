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

# Exceptions (BacenAnalysisError = base para capturar todas)
from ifdata_bcb.domain.exceptions import (
    BacenAnalysisError,
    DataUnavailableError,
)

# Funcoes de alto nivel
from ifdata_bcb.core.api import search

# Lazy loading dos explorers
_cosif = None
_ifdata = None
_cadastro = None


def __getattr__(name: str):
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
            from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer

            _cadastro = CadastroExplorer()
        return _cadastro

    raise AttributeError(f"module 'ifdata_bcb' has no attribute '{name}'")


__all__ = [
    # Explorers (lazy)
    "cosif",
    "ifdata",
    "cadastro",
    # Funcoes de alto nivel
    "search",
    # Exceptions (BacenAnalysisError = base)
    "BacenAnalysisError",
    "DataUnavailableError",
]
