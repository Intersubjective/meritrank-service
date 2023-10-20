import asyncio
from os import getenv

from fastapi import FastAPI

from meritrank_service import __version__ as meritrank_service_version

from meritrank_service.graphql import get_graphql_app
from meritrank_service.gravity_rank import GravityRank
from meritrank_service.log import LOGGER
from meritrank_service.postgres_edges_updater import create_notification_listener
from meritrank_service.rest import MeritRankRestRoutes
from meritrank_service.settings import MeritRankSettings


def create_meritrank_app():
    edges_data = None
    settings = MeritRankSettings()
    LOGGER.setLevel(settings.log_level)

    if settings.pg_dsn:
        from meritrank_service.postgres_edges_provider import get_edges_data
        LOGGER.info("Got POSTGRES_DB_URL env variable, connecting DB to get initial data ")
        edges_data = get_edges_data(settings.pg_dsn)
        LOGGER.info("Loaded edges from DB")

    LOGGER.info("Creating meritrank instance")
    rank_instance = GravityRank(graph=edges_data, logger=LOGGER.getChild("meritrank"))
    user_routes = MeritRankRestRoutes(rank_instance)

    LOGGER.info("Creating FastAPI instance")
    app = FastAPI(title="MeritRank", version=meritrank_service_version)
    app.include_router(user_routes.router)
    app.include_router(get_graphql_app(rank_instance), prefix="/graphql")
    LOGGER.info("Returning app instance")

    @app.on_event("startup")
    async def startup_event():
        if settings.pg_edges_channel:
            LOGGER.info("Starting LISTEN to Postgres")
            app.state.edges_updater_task = asyncio.create_task(
                create_notification_listener(settings.pg_dsn, settings.pg_edges_channel, rank_instance.add_edge))
        if settings.ego_warmup:
            LOGGER.info("Scheduling ego warmup")

    @app.on_event("shutdown")
    async def shutdown_event():
        if app.state.edges_updater_task is None:
            return
        LOGGER.info("Stopping LISTEN to Postgres")
        app.state.edges_updater_task.cancel()
        await app.state.edges_updater_task

    return app
