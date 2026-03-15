"""
Configuracao de paths e constantes para ifdata-bcb.

Este modulo centraliza a configuracao de diretorios usados pelo pacote,
permitindo customizacao via variavel de ambiente.
"""

import os
from pathlib import Path
from platformdirs import user_cache_dir

# Nome da aplicacao para diretorio de cache
APP_NAME = "py-bacen"


def get_cache_path() -> Path:
    """
    Retorna o caminho para cache de dados.

    Prioridade:
    1. Variavel de ambiente BACEN_DATA_DIR
    2. Diretorio de cache do sistema (XDG/AppData)

    Estrutura (Windows):
        AppData/Local/py-bacen/
            Cache/
                cosif/
                    individual/
                    prudencial/
                ifdata/
                    valores/
                    cadastro/
            Logs/

    Returns:
        Path para o diretorio de cache.
    """
    env_path = os.environ.get("BACEN_DATA_DIR")
    if env_path:
        path = Path(env_path)
    else:
        # user_cache_dir no Windows retorna AppData/Local/py-bacen/Cache
        path = Path(user_cache_dir(APP_NAME, appauthor=False))

    path.mkdir(parents=True, exist_ok=True)
    return path


def get_logs_path() -> Path:
    """
    Retorna o caminho para diretorio de logs.

    Logs sao armazenados separadamente do cache de dados.

    Estrutura (Windows):
        AppData/Local/py-bacen/Logs/

    Returns:
        Path para o diretorio de logs.
    """
    # Pega o parent do cache (py-bacen) para criar Logs no mesmo nivel
    cache_path = Path(user_cache_dir(APP_NAME, appauthor=False))
    logs_path = cache_path.parent / "Logs"
    logs_path.mkdir(parents=True, exist_ok=True)
    return logs_path


# Estrutura de subdiretorios padrao
SUBDIRS = {
    "cosif_individual": "cosif/individual",
    "cosif_prudencial": "cosif/prudencial",
    "ifdata_valores": "ifdata/valores",
    "ifdata_cadastro": "ifdata/cadastro",
}


def get_subdir(dataset: str) -> str:
    """
    Retorna o subdiretorio para um dataset especifico.

    Args:
        dataset: Nome do dataset (cosif_individual, cosif_prudencial,
                 ifdata_valores, ifdata_cadastro)

    Returns:
        String com o subdiretorio relativo.

    Raises:
        KeyError: Se o dataset nao for reconhecido.
    """
    if dataset not in SUBDIRS:
        valid = ", ".join(SUBDIRS.keys())
        raise KeyError(f"Dataset '{dataset}' desconhecido. Validos: {valid}")
    return SUBDIRS[dataset]


def ensure_subdir(dataset: str) -> Path:
    """
    Garante que o subdiretorio existe e retorna o Path completo.

    Args:
        dataset: Nome do dataset.

    Returns:
        Path completo para o subdiretorio.
    """
    subdir = get_subdir(dataset)
    path = get_cache_path() / subdir
    path.mkdir(parents=True, exist_ok=True)
    return path
