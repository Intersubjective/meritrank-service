from classy_fastapi import Routable, get, put
from pydantic import BaseModel

from meritrank_python.rank import NodeId
from meritrank_python.lazy import LazyMeritRank

from meritrank_service.log import LOGGER as TOPLEVEL_LOGGER


class Edge(BaseModel):
    src: NodeId
    dest: NodeId
    weight: float = 1.0


class NodeScore(BaseModel):
    node: NodeId
    ego: NodeId
    score: float


LOGGER = TOPLEVEL_LOGGER.getChild("REST")


class MeritRankRestRoutes(Routable):
    def __init__(self, rank: LazyMeritRank) -> None:
        super().__init__()
        self.__rank = rank
        LOGGER.info("Created REST router")

    @get("/healthcheck")
    async def healthcheck(self):
        # Basic healthcheck route for Docker integration
        return {"status": "ok"}

    @put("/calculate")
    async def calculate(self, ego: NodeId, count: int = 10000):
        """
        (Re)initialze an ego by (re)calculating walks for it.
        :param ego: the node to create ego for
        :param count: the number of walks to generate for the ego
        """
        self.__rank.calculate(ego, num_walks=count)
        return {"message": f"Calculated {count} walks for {ego}"}

    @put("/loglevel")
    async def loglevel(self, loglevel: str):
        """
        Set global logger level
        :param loglevel: logging level in Python notation (e.g. DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        TOPLEVEL_LOGGER.setLevel(loglevel.upper())
        LOGGER.info("Changed log level to %s", loglevel)
        return {"message": f"Set log level to {loglevel}"}

    @get("/edge/{src}/{dest}")
    async def get_edge(self, src: NodeId, dest: NodeId):
        if (weight := self.__rank.get_edge(src, dest)) is not None:
            return Edge(src=src, dest=dest, weight=weight)

    @put("/edge")
    async def put_edge(self, edge: Edge):
        self.__rank.add_edge(edge.src, edge.dest, edge.weight)
        LOGGER.info("Added edge: (%s, %s, %f)", edge.src, edge.dest, edge.weight)
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

        self.__rank = LazyMeritRank(graph)
        return {"message": f"Added {len(edges_list)} edges"}

    @get("/scores/{ego}")
    async def get_scores(self, ego: NodeId, limit: int | None = None) -> list[NodeScore]:
        return [NodeScore(node=node, ego=ego, score=score) for node, score in
                self.__rank.get_ranks(ego, limit=limit).items()]

    @get("/node_score/{ego}/{node}")
    async def get_node_score(self, ego: NodeId, node: NodeId) -> NodeScore:
        return NodeScore(node=node, ego=ego, score=self.__rank.get_node_score(ego, node))

    @get("/node_edges/{src}")
    async def get_node_edges(self, src: NodeId) -> list[Edge]:
        return list(Edge(src=e[0], dest=e[1], weight=e[2]) for e in self.__rank.get_node_edges(src))
