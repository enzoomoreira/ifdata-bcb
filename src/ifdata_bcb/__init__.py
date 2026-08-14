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

from typing import TYPE_CHECKING, Any

from loguru import logger

# Excecoes e warnings importados diretamente (nao passam por
# domain/__init__.py). O modulo nao importa nada, entao expor o conjunto
# completo aqui nao custa o pandas/duckdb que o lazy loading evita -- e
# tratar erro ou filtrar warning deixa de exigir conhecer o layout interno.
from ifdata_bcb.domain.exceptions import (
    BacenAnalysisError,
    BacenWarning,
    DataProcessingError,
    DataUnavailableError,
    DroppedReportWarning,
    EmptyFilterWarning,
    IncompatibleEraWarning,
    InvalidColumnError,
    InvalidDateFormatError,
    InvalidDateRangeError,
    InvalidIdentifierError,
    InvalidScopeError,
    MissingRequiredParameterError,
    NullValuesWarning,
    PartialDataWarning,
    PeriodUnavailableError,
    ScopeMigrationWarning,
    ScopeUnavailableWarning,
    TruncatedResultWarning,
)

if TYPE_CHECKING:
    # Declaracoes so para o type checker: em runtime os nomes continuam
    # resolvendo por __getattr__ (anotacao de modulo nao vincula nome), mas
    # sem isto o checker enxerga bcb.cosif como Any e nao ha autocomplete de
    # read()/collect()/search() -- justamente no ponto de entrada principal.
    from ifdata_bcb.core.eras import EraDiagnostic, GrupoEra
    from ifdata_bcb.infra.log import disable_logging, enable_logging
    from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
    from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
    from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer

    cosif: COSIFExplorer
    ifdata: IFDATAExplorer
    cadastro: CadastroExplorer

# Biblioteca nao loga por padrao: o logger global do loguru pertence a aplicacao
# consumidora. Consumidor ativa com enable_logging().
# Feito aqui (e nao em infra.log) porque importar infra puxa pandas/duckdb.
logger.disable("ifdata_bcb")

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

    if name in ("enable_logging", "disable_logging"):
        import ifdata_bcb.infra.log as log_module

        return getattr(log_module, name)

    if name in ("EraDiagnostic", "GrupoEra"):
        import ifdata_bcb.core.eras as eras_module

        return getattr(eras_module, name)

    raise AttributeError(f"module 'ifdata_bcb' has no attribute '{name}'")


def __dir__() -> list[str]:
    return list(__all__) + list(globals().keys())


__all__ = [
    # Explorers (lazy)
    "cosif",
    "ifdata",
    "cadastro",
    # Excecoes (BacenAnalysisError = base de todas)
    "BacenAnalysisError",
    "DataProcessingError",
    "DataUnavailableError",
    "InvalidColumnError",
    "InvalidDateFormatError",
    "InvalidDateRangeError",
    "InvalidIdentifierError",
    "InvalidScopeError",
    "MissingRequiredParameterError",
    "PeriodUnavailableError",
    # Warnings (BacenWarning = base de todos; filtre por ela para silenciar tudo)
    "BacenWarning",
    "DroppedReportWarning",
    "EmptyFilterWarning",
    "IncompatibleEraWarning",
    "NullValuesWarning",
    "PartialDataWarning",
    "ScopeMigrationWarning",
    "ScopeUnavailableWarning",
    "TruncatedResultWarning",
    # Diagnostico de era retornado por check_era() e em df.attrs['era']
    "EraDiagnostic",
    "GrupoEra",
    # Logging (desativado por padrao)
    "enable_logging",
    "disable_logging",
]
