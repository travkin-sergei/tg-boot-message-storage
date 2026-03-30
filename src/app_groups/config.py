# src/app_groups/models.py
"""
Конфигурация микросервиса app_groups.
Совместимо с Python 3.12+ и Pydantic V2.
"""
from pydantic_settings import BaseSettings

# === Константы на уровне модуля (для импорта в models.py и main.py) ===
DB_SCHEMA: str = "app_groups"
APP_NAME: str = "app_groups"
API_PREFIX: str = "/api/v1/groups"
VERSION: str = "1.0.0"

class AppGroupsConfig(BaseSettings):
    """Настройки приложения app_groups."""

    # Идентификаторы (дублируем константы для доступа через объект config)
    DB_SCHEMA: str = DB_SCHEMA
    APP_NAME: str = APP_NAME
    API_PREFIX: str = API_PREFIX
    VERSION: str = VERSION

    # Подключение к БД (имя конфигурации в database.py)
    DB_CONNECTION_NAME: str = "app_google_target"

    # Логирование
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/app_groups.log"

    # Сервер
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    RELOAD: bool = True

    model_config = {
        "env_prefix": "APP_GROUPS_",
        "case_sensitive": True,
        "extra": "ignore"
    }

# Глобальный экземпляр конфигурации
config = AppGroupsConfig()