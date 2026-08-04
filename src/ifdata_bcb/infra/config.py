from functools import lru_cache
from pathlib import Path

from platformdirs import user_cache_dir
from pydantic_settings import BaseSettings, SettingsConfigDict

APP_NAME = "py-bacen"


class Settings(BaseSettings):
    """
    Configuracao da biblioteca, sobrescrevivel por variaveis de ambiente.

    Todas usam o prefixo BACEN_ (ex: BACEN_DATA_DIR, BACEN_MAX_WORKERS).
    """

    model_config = SettingsConfigDict(env_prefix="BACEN_")

    data_dir: Path = Path(user_cache_dir(APP_NAME, appauthor=False))
    max_workers: int = 4
    request_timeout: float = 240.0
    connect_timeout: float = 10.0
    fuzzy_threshold: int = 78

    @property
    def cache_path(self) -> Path:
        return self.data_dir

    @property
    def logs_path(self) -> Path:
        # Dentro do data_dir, nao ao lado: com data_dir.parent, um cache em
        # ~/.cache/py-bacen jogava os logs direto em ~/.cache/Logs, e um
        # BACEN_DATA_DIR=/dados/bcb os jogava fora do diretorio configurado.
        return self.data_dir / "logs"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Settings do processo.

    Cacheado: use get_settings.cache_clear() apos alterar variaveis de
    ambiente em runtime (ou em testes com monkeypatch).
    """
    return Settings()
