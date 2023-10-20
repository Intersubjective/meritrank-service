import logging
from typing import Optional

from pydantic import BaseSettings, PostgresDsn, Field, validator, root_validator


class MeritRankSettings(BaseSettings):
    pg_dsn: Optional[PostgresDsn] = Field(env="POSTGRES_DB_URL")
    log_level: Optional[str] = Field(env="MERITRANK_DEBUG_LEVEL")
    pg_edges_channel: Optional[str] = Field(env="POSTGRES_EDGES_CHANNEL")
    ego_warmup: bool = False

    @validator('log_level')
    @classmethod
    def validate_log_level(cls, v):
        valid_log_levels = [logging.getLevelName(level) for level in
                            (logging.DEBUG,
                             logging.INFO,
                             logging.WARNING,
                             logging.ERROR,
                             logging.CRITICAL)]

        if v.upper() not in valid_log_levels:
            raise ValueError(f'Invalid log level. Allowed values are {valid_log_levels}')

        return v.upper()  # return the validated and normalized log level

    @root_validator
    @classmethod
    def check_consistency(cls, values):
        if values.get('pg_dsn') is None:
            if values.get('ego_warmup'):
                raise ValueError('Ego warmup feature requires a Postgres DSN')
            if values.get("pg_edges_channel"):
                raise ValueError('Postgres edges option (SQL LISTEN/NOTIFY) requires a Postgres DSN')
        return values
