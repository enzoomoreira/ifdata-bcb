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
    bcb.cadastro.collect('2024-01', '2024-12')  # necessario para cadastro= no read()

    # Consultas usando CNPJ de 8 digitos
    # start e OBRIGATORIO; instituicao e opcional (None = todas)
    # start sozinho = data unica; start + end = range de datas
    df = bcb.ifdata.read(
        '2024-12',  # start (posicional, obrigatorio)
        instituicao='60872504',  # keyword-only, opcional
        conta='Lucro Liquido',
    )

    df = bcb.cosif.read(
        instituicao=['60872504', '60746948'],
        start='2024-01',
        end='2024-12',  # Range de datas
        conta=['TOTAL GERAL DO ATIVO', 'PATRIMONIO LIQUIDO'],
    )  # escopo=None busca em todos os escopos
"""

from typing import Any

# Exceptions importadas diretamente (nao passam por domain/__init__.py)
from ifdata_bcb.domain.exceptions import (
    BacenAnalysisError,
    DataUnavailableError,
)

# Lazy loading de tudo que puxa pandas/duckdb
_cosif = None
_ifdata = None
_cadastro = None


def __getattr__(name: str) -> Any:
    """Lazy loading dos explorers."""
    global _cosif, _ifdata, _cadastro

    if name == "cosif":
        if _cosif is None:
            from ifdata_bcb.providers.cosif.explorer import COSIFExplorer

            _cosif = COSIFExplorer()
        return _cosif

    if name == "ifdata":
        if _ifdata is None:
            from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer

            _ifdata = IFDATAExplorer()
        return _ifdata

    if name == "cadastro":
        if _cadastro is None:
            from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer

            _cadastro = CadastroExplorer()
        return _cadastro

    raise AttributeError(f"module 'ifdata_bcb' has no attribute '{name}'")


def __dir__() -> list[str]:
    return list(__all__) + list(globals().keys())


__all__ = [
    # Explorers (lazy)
    "cosif",
    "ifdata",
    "cadastro",
    # Exceptions (BacenAnalysisError = base)
    "BacenAnalysisError",
    "DataUnavailableError",
]
