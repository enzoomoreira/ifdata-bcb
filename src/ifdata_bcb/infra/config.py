from pathlib import Path

from platformdirs import user_cache_dir
from pydantic_settings import BaseSettings, SettingsConfigDict

APP_NAME = "py-bacen"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BACEN_")

    data_dir: Path = Path(user_cache_dir(APP_NAME, appauthor=False))

    @property
    def cache_path(self) -> Path:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        return self.data_dir

    @property
    def logs_path(self) -> Path:
        path = self.data_dir.parent / "Logs"
        path.mkdir(parents=True, exist_ok=True)
        return path


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
