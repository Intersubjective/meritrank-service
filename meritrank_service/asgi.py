import asyncio

from fastapi import FastAPI

from meritrank_service import __version__ as meritrank_service_version

from meritrank_service.graphql import get_graphql_app
from meritrank_service.gravity_rank import GravityRank
from meritrank_service.log import LOGGER
from meritrank_service.postgres_edges_updater import create_notification_listener
from meritrank_service.rest import MeritRankRestRoutes
from meritrank_service.settings import MeritRankSettings
from meritrank_service.fdw_parser import create_fdw_listener


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
    mr = GravityRank(graph=edges_data, logger=LOGGER.getChild("meritrank"))
    user_routes = MeritRankRestRoutes(mr)

    LOGGER.info("Creating FastAPI instance")
    app = FastAPI(title="MeritRank", version=meritrank_service_version)
    app.include_router(user_routes.router)
    app.include_router(get_graphql_app(mr), prefix="/graphql")
    LOGGER.info("Returning app instance")

    @app.on_event("startup")
    async def startup_event():

        if settings.pg_fdw_listen_url:
            LOGGER.info("Starting PyNNG listener for Postgres FDW")
            app.state.fdw_listener_task = asyncio.create_task(
                create_fdw_listener(mr, settings.pg_fdw_listen_url))

        if settings.pg_edges_channel:
            LOGGER.info("Starting LISTEN to Postgres")
            app.state.edges_updater_task = asyncio.create_task(
                create_notification_listener(
                    settings.pg_dsn,
                    settings.pg_edges_channel,
                    mr.add_edge))

        if settings.ego_warmup:
            LOGGER.info("Scheduling ego warmup")
            async def warmup_into_zero():
                if settings.zero_node:
                    LOGGER.info("Scheduling zero heartbeat to start after warmup")
                if settings.ego_warmup:
                    LOGGER.info("Scheduling ego warmup")
                    await rank_instance.warmup(settings.ego_warmup_wait)
                if settings.zero_node:
                    await rank_instance.zero_opinion_heartbeat(
                        settings.zero_node,
                        settings.zero_top_nodes_limit,
                        settings.zero_heartbeat_period)

            app.state.ego_warmup_task = asyncio.create_task(warmup_into_zero())

    @app.on_event("shutdown")
    async def shutdown_event():
        if app.state.ego_warmup_task and app.state.ego_warmup_task.running():
            LOGGER.info("Warmup task still running, cancelling")
            app.state.ego_warmup_task.cancel()
            await app.state.ego_warmup_task

        if app.state.edges_updater_task:
            LOGGER.info("Stopping LISTEN to Postgres")
            app.state.edges_updater_task.cancel()
            await app.state.edges_updater_task

        if app.state.pg_fdw_listen_url:
            LOGGER.info("Stopping PyNNG listener for Postgres FDW")
            app.state.fdw_listener_task.cancel()
            await app.state.fdw_listener_task

    return app
