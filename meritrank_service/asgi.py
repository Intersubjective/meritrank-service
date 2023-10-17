import asyncio
from os import getenv

from fastapi import FastAPI

from meritrank_service import __version__ as meritrank_service_version

from meritrank_service.graphql import get_graphql_app
from meritrank_service.gravity_rank import GravityRank
from meritrank_service.log import LOGGER
from meritrank_service.postgres_edges_updater import create_notification_listener
from meritrank_service.rest import MeritRankRestRoutes


def create_meritrank_app():
    edges_data = None
    if debug_level := getenv("MERITRANK_DEBUG_LEVEL"):
        LOGGER.setLevel(debug_level.upper())
    postgres_edges_channel = getenv("POSTGRES_EDGES_CHANNEL")

    if postgres_url := getenv("POSTGRES_DB_URL"):
        from meritrank_service.postgres_edges_provider import get_edges_data
        LOGGER.info("Got POSTGRES_DB_URL env variable, connecting DB to get initial data ")
        edges_data = get_edges_data(postgres_url)
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
        if postgres_url and postgres_edges_channel:
            LOGGER.info("Starting LISTEN to Postgres")
            app.state.edges_updater_task = asyncio.create_task(
                create_notification_listener(postgres_url, postgres_edges_channel, rank_instance.add_edge))

    @app.on_event("shutdown")
    async def shutdown_event():
        if app.state.edges_updater_task is None:
            return
        LOGGER.info("Stopping LISTEN to Postgres")
        app.state.edges_updater_task.cancel()
        await app.state.edges_updater_task

    return app
