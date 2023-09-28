from os import getenv

from classy_fastapi import Routable, get, put
from fastapi import FastAPI
from pydantic import BaseModel

from meritrank_python.rank import NodeId, IncrementalMeritRank
from meritrank_service import __version__ as meritrank_service_version


class Edge(BaseModel):
    src: NodeId
    dest: NodeId
    weight: float = 1.0


class NodeScore(BaseModel):
    node: NodeId
    ego: NodeId
    score: float


class MeritRankRoutes(Routable):
    def __init__(self, rank: IncrementalMeritRank) -> None:
        super().__init__()
        self.__rank = rank
        # The set of egos for which MeritRank has already been calculated
        self.__egos = set()

    @get("/healthcheck")
    async def healthcheck(self):
        # Basic healthcheck route for Docker integration
        return {"status": "ok"}

    @get("/edge/{src}/{dest}")
    async def get_edge(self, src: NodeId, dest: NodeId):
        if (weight := self.__rank.get_edge(src, dest)) is not None:
            return Edge(src=src, dest=dest, weight=weight)

    @put("/edge")
    async def put_edge(self, edge: Edge):
        self.__rank.add_edge(edge.src, edge.dest, edge.weight)
        return {"message": f"Added edge {edge.src} -> {edge.dest} "
                           f"with weight {edge.weight}"}

    @put("/graph")
    async def put_graph(self, edges_list: list[Edge]):
        # Replace the existing MeritRank instance with a new one,
        # initialized from the given graph
        graph = {}
        for edge in edges_list:
            graph.setdefault(edge.src, {}).setdefault(edge.dest, {})[
                'weight'] = edge.weight

        self.__rank = IncrementalMeritRank(graph)
        return {"message": f"Added {len(edges_list)} edges"}

    @get("/scores/{ego}")
    async def get_scores(self, ego: NodeId, limit: int | None = None) -> list[NodeScore]:
        self.__maybe_add_ego(ego)
        return [NodeScore(node=node, ego=ego, score=score) for node, score in
                self.__rank.get_ranks(ego, limit=limit).items()]

    @get("/node_score/{ego}/{node}")
    async def get_node_score(self, ego: NodeId, node: NodeId)-> NodeScore:
        self.__maybe_add_ego(ego)
        return NodeScore(node=node, ego=ego, score=self.__rank.get_node_score(ego, node))

    @get("/node_edges/{src}")
    async def get_node_edges(self, src: NodeId) -> list[Edge]:
        return list(Edge(src=e[0], dest=e[1], weight=e[2]) for e in self.__rank.get_node_edges(src))

    def __maybe_add_ego(self, ego):
        if ego not in self.__egos:
            self.__egos.add(ego)
            self.__rank.calculate(ego)


app = FastAPI(title="MeritRank", version=meritrank_service_version)


def create_meritrank_app():
    edges_data = None
    if postgres_url := getenv("POSTGRES_DB_URL"):
        from meritrank_service.postgres_edges_provider import get_edges_data
        edges_data = get_edges_data(postgres_url)
    rank_instance = IncrementalMeritRank(graph=edges_data)
    user_routes = MeritRankRoutes(rank_instance)

    app.include_router(user_routes.router)
    return app
