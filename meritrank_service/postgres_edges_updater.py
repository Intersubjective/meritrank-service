import asyncpg_listen

from meritrank_service.log import LOGGER
from meritrank_service.rest import Edge


def create_notification_listener(postgres_url, channel_name, callback):
    listener = asyncpg_listen.NotificationListener(asyncpg_listen.connect_func(dsn=postgres_url))

    async def handle_notifications(notification: asyncpg_listen.NotificationOrTimeout) -> None:
        if isinstance(notification, asyncpg_listen.Timeout):
            LOGGER.warning("Timeout waiting for notification from Postgres")
            return
        e = Edge.parse_raw(notification.payload)
        LOGGER.debug("Received notification from Postgres: %s", notification.payload)
        callback(e.src, e.dest, e.weight)

    return listener.run(
        {channel_name: handle_notifications},
        policy=asyncpg_listen.ListenPolicy.ALL,
        notification_timeout=3600
    )
