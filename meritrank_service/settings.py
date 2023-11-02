import logging
from typing import Optional

from pydantic import BaseSettings, PostgresDsn, Field, validator, root_validator

class NNGUrl(AnyUrl):
    allowed_schemes = {'inproc', 'ipc', 'tcp', 'ws', 'tls+tcp', 'wss'}


    __slots__ = ()
class MeritRankSettings(BaseSettings):
    pg_dsn: Optional[PostgresDsn] = Field(env="POSTGRES_DB_URL")
    log_level: Optional[str] = Field(env="MERITRANK_DEBUG_LEVEL")
    pg_edges_channel: Optional[str] = Field(env="POSTGRES_EDGES_CHANNEL")
    ego_warmup: bool = False
    ego_warmup_wait: int = 0  # Time to wait before starting the warmup
    zero_node: Optional[str] = None
    zero_top_nodes_limit: int = 1000
    zero_heartbeat_period: int = 60*60  # Seconds to wait before refreshing zero's opinion on network
    pg_fdw_listen_url: Optional[NNGUrl] = "tcp://127.0.0.1:10234"

    @validator('log_level')
    @classmethod
    def validate_log_level(cls, v):
        if v is None:
            return logging.INFO
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
