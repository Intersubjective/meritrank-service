from functools import wraps
from typing import Optional

import strawberry
from fastapi import Depends
from meritrank_python.rank import IncrementalMeritRank, NodeDoesNotExist, EgoNotInitialized, EgoCounterEmpty
from strawberry import UNSET

from strawberry.fastapi import GraphQLRouter, BaseContext
from strawberry.types import Info

from meritrank_service.gql_types import Edge, NodeScore, GravityGraph
from meritrank_service.gravity_rank import GravityRank
from meritrank_service.log import LOGGER


def handle_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except NodeDoesNotExist:
            raise ValueError("Tried to get score from the standpoint of non-existing node", args[1])
        except EgoNotInitialized:
            raise ValueError("Tried to get score from node that was not initialized as ego before", args[1])
        except EgoCounterEmpty:
            raise ValueError("Score counter empty for ego. (e.g. initialized with zero walks)", args[1])

    return wrapper


@strawberry.input
class NodeInput:
    like: Optional[str] = UNSET

    def match(self, node) -> bool:
        if self.like is not UNSET:
            return node.startswith(self.like)
        return True


@strawberry.input
class ScoreInput:
    lte: Optional[float] = UNSET
    lt: Optional[float] = UNSET
    gte: Optional[float] = UNSET
    gt: Optional[float] = UNSET

    def match(self, score) -> bool:
        if (lte := self.lte) is not UNSET and not (score <= lte):
            return False
        if (lt := self.lt) is not UNSET and not (score < lt):
            return False
        if (gte := self.gte) is not UNSET and not (score >= gte):
            return False
        if (gt := self.gt) is not UNSET and not (score > gt):
            return False
        return True


@strawberry.input
class NodeScoreWhereInput:
    node: Optional[NodeInput] = UNSET
    score: Optional[ScoreInput] = UNSET

    def match(self, node, score) -> bool:
        if self.node is not UNSET and not self.node.match(node):
            return False
        if self.score is not UNSET and not self.score.match(score):
            return False
        return True


LOGGER = LOGGER.getChild("graphql")


def ego_score_dict_to_list(ego, d):
    return [NodeScore(node=n, ego=ego, score=s) for n, s in d.items()]


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
    @handle_exceptions
    def score(self, info, ego: str, node: str) -> NodeScore:
        score = info.context.mr.get_node_score(ego, node)
        return NodeScore(node=node, ego=ego, score=score)

    @strawberry.field
    @handle_exceptions
    def scores(self, info, ego: str,
               where: Optional[NodeScoreWhereInput] = UNSET,
               limit: Optional[int] = UNSET) -> list[NodeScore]:
        result = []
        for node, score in info.context.mr.get_ranks(ego, limit=limit or None).items():
            if where is not UNSET and not where.match(node, score):
                continue
            result.append(NodeScore(node=node, ego=ego, score=score))
        return result

    @strawberry.field
    def gravity_graph(self, info, ego: str,
                      focus: Optional[str] = UNSET,
                      min_abs_score: Optional[float] = UNSET,
                      positive_only: Optional[bool] = UNSET,
                      recurse_depth: Optional[int] = UNSET,
                      ) -> GravityGraph:
        """
        This handle returns a graph of user's connections to other users.
        The graph is specific to usage in the Gravity/A2 social network.
        """
        LOGGER.info("Getting gravity graph (%s, include_negative=%s)", ego, "True" if positive_only else "False")
        edges, users, beacons, comments = info.context.mr.gravity_graph_filtered(
            ego, [focus or ego],
            min_abs_score or None,
            positive_only or None,
            recurse_depth or 2)
        return GravityGraph(
            edges=edges,
            users=ego_score_dict_to_list(ego, users),
            beacons=ego_score_dict_to_list(ego, beacons),
            comments=ego_score_dict_to_list(ego, comments)
        )


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


def get_graphql_app(rank: GravityRank):
    def get_meritrank_instance():
        return CustomContext(rank)

    async def get_context(custom_context=Depends(get_meritrank_instance)):
        return custom_context

    graphql_app = GraphQLRouter(schema, context_getter=get_context)
    LOGGER.info("Created GraphQL router")
    return graphql_app
