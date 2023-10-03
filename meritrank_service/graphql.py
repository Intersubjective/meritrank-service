from typing import Optional

import strawberry
from fastapi import Depends
from meritrank_python.rank import IncrementalMeritRank, NodeDoesNotExist

from strawberry.fastapi import GraphQLRouter, BaseContext
from strawberry.types import Info

from meritrank_service.asgi import LazyMeritRank
from meritrank_service.log import LOGGER


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
    def scores(self, info, ego: str, limit: int | None = None) -> list[NodeScore]:
        try:
            return [NodeScore(node=node, ego=ego, score=score) for node, score in
                    info.context.mr.get_ranks(ego, limit=limit).items()]
        except NodeDoesNotExist:
            raise ValueError("Tried to get score for non-existing ego", ego)


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