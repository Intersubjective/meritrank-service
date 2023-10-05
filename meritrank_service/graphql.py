from typing import Optional

import strawberry
from fastapi import Depends
from meritrank_python.rank import IncrementalMeritRank, NodeDoesNotExist

from strawberry.fastapi import GraphQLRouter, BaseContext
from strawberry.types import Info

from meritrank_service.asgi import LazyMeritRank
from meritrank_service.log import LOGGER


@strawberry.input
class NodeInput:
    like: Optional[str] = None

    def match(self, node) -> bool:
        if self.like is not None:
            return node.startswith(self.like)
        return True


@strawberry.input
class ScoreInput:
    lte: Optional[float] = None
    lt: Optional[float] = None
    gte: Optional[float] = None
    gt: Optional[float] = None

    def match(self, score) -> bool:
        if (lte := self.lte) is not None and not (score <= lte):
            return False
        if (lt := self.lt) is not None and not (score < lt):
            return False
        if (gte := self.gte) is not None and not (score >= gte):
            return False
        if (gt := self.gt) is not None and not (score > gt):
            return False
        return True


@strawberry.input
class NodeScoreWhereInput:
    node: Optional[NodeInput] = None
    score: Optional[ScoreInput] = None

    def match(self, node, score) -> bool:
        if self.node is not None and not self.node.match(node):
            return False
        if self.score is not None and not self.score.match(score):
            return False
        return True


@strawberry.type
class NodeScore:
    node: str
    ego: str
    score: float


@strawberry.type
class Edge:
    src: str
    dest: str
    weight: float


LOGGER = LOGGER.getChild("graphql")


@strawberry.type
class Query:
    @strawberry.field
    def edge(self, info: Info, src: str, dest: str) -> Optional[Edge]:
        if (weight := info.context.mr.get_edge(src, dest)) is not None:
            return Edge(src=src, dest=dest, weight=weight)
        return None

    @strawberry.field
    def edges(self, info: Info, src: str) -> list[Edge]:
        return [Edge(src=e[0], dest=e[1], weight=e[2]) for e in info.context.mr.get_node_edges(src)]

    @strawberry.field
    def score(self, info, ego: str, node: str) -> NodeScore:
        try:
            score = info.context.mr.get_node_score(ego, node)
        except NodeDoesNotExist:
            raise ValueError("Tried to get score for non-existing ego", ego)
        return NodeScore(node=node, ego=ego, score=score)

    @strawberry.field
    def scores(self, info, ego: str,
               where: NodeScoreWhereInput | None = None,
               limit: int | None = None) -> list[NodeScore]:
        result = []
        try:
            for node, score in info.context.mr.get_ranks(ego, limit=limit).items():
                if where is not None and not where.match(node, score):
                    continue
                result.append(NodeScore(node=node, ego=ego, score=score))
        except NodeDoesNotExist:
            raise ValueError("Tried to get score for non-existing ego", ego)
        return result


@strawberry.type
class Mutation:
    @strawberry.mutation
    def put_edge(self, info: Info, src: str, dest: str, weight: float) -> Edge:
        info.context.mr.add_edge(src, dest, weight)
        LOGGER.info("Added edge: (%s, %s, %f)", src, dest, weight)
        return Edge(src=src, dest=dest, weight=weight)


class CustomContext(BaseContext):
    def __init__(self, rank: IncrementalMeritRank):
        super().__init__()
        self.mr: IncrementalMeritRank = rank


schema = strawberry.Schema(Query, Mutation)


def get_graphql_app(rank: LazyMeritRank):
    def get_meritrank_instance():
        return CustomContext(rank)

    async def get_context(custom_context=Depends(get_meritrank_instance)):
        return custom_context

    graphql_app = GraphQLRouter(schema, context_getter=get_context)
    LOGGER.info("Created GraphQL router")
    return graphql_app
