from functools import wraps
from typing import Optional

import strawberry
from fastapi import Depends
from meritrank_python.rank import IncrementalMeritRank, NodeDoesNotExist, EgoNotInitialized, EgoCounterEmpty
from strawberry import UNSET

from strawberry.fastapi import GraphQLRouter, BaseContext
from strawberry.types import Info

from meritrank_service.error_gql_schema import ErrorEnabledSchema
from meritrank_service.gql_types import Edge, NodeScore, GravityGraph, MutualScore
from meritrank_service.gravity_rank import GravityRank
from meritrank_service.log import LOGGER


def handle_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        args_str = ', '.join(str(arg) for arg in args)
        kwargs_str = ', '.join(f'{k}={str(v)}' for k, v in kwargs.items() if k != "info")
        try:
            return func(*args, **kwargs)
        except NodeDoesNotExist as e:
            LOGGER.warning('GQL query "%s" exception: node %s does not exist. args %s, kwargs %s', func.__name__,
                           e.node, args_str, kwargs_str)
            return None
        except EgoNotInitialized:
            raise ValueError("Tried to get score from node that was not initialized as ego before")
        except EgoCounterEmpty:
            raise ValueError("Score counter empty for ego. (e.g. initialized with zero walks)")

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

def demux_nodes(nodes_dict):
    users, beacons, comments = {}, {}, {}
    for node, score in nodes_dict.items():
        match node[0]:
            case "U":
                users[node] = score
            case "B":
                beacons[node] = score
            case "C":
                comments[node] = score
    return users, beacons, comments



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
    def score(self, info, ego: str, node: str) -> Optional[NodeScore]:
        score = info.context.mr.get_node_score(ego, node)
        return NodeScore(node=node, ego=ego, score=score)

    @strawberry.field
    def scores(self, info, ego: str,
               where: Optional[NodeScoreWhereInput] = UNSET,
               limit: Optional[int] = UNSET,
               hide_personal: Optional[bool] = UNSET
               ) -> list[NodeScore]:
        result = []
        for node, score in info.context.mr.get_ranks(ego, limit=limit or None).items():
            if where is not UNSET and not where.match(node, score):
                continue
            if (hide_personal
                    and (node.startswith("C") or node.startswith("B"))
                    and info.context.mr.get_edge(node, ego)):
                continue

            try:
                result.append(NodeScore(node=node, ego=ego, score=score))
            except NodeDoesNotExist:
                self.logger.warning('GQL query "score" exception: node %s does not exist.', node)
        return result

    @strawberry.field
    def gravity_graph(self, info, ego: str,
                      focus: Optional[str] = UNSET,
                      positive_only: Optional[bool] = UNSET,
                      limit: Optional[int] = UNSET
                      ) -> GravityGraph:
        """
        This handle returns a graph of user's connections to other users.
        The graph is specific to usage in the Gravity/A2 social network.
        """
        LOGGER.info("Getting gravity graph (%s, include_negative=%s)", ego, "True" if positive_only else "False")
        edges, nodes_dict = info.context.mr.gravity_graph(
            ego, focus or ego,
            positive_only if positive_only is not UNSET else True,
            limit if limit is not UNSET else None
        )
        users, beacons, comments = demux_nodes(nodes_dict)
        return GravityGraph(
            edges=edges,
            users=ego_score_dict_to_list(ego, users),
            beacons=ego_score_dict_to_list(ego, beacons),
            comments=ego_score_dict_to_list(ego, comments)
        )

    @strawberry.field
    def users_stats(self, info, ego: str) -> list[MutualScore]:
        LOGGER.info("Getting users stats for user %s", ego)
        stats_dict  = info.context.mr.get_users_stats(ego)
        return [MutualScore(ego=ego, node=k, node_score=v[0], ego_score=v[1]) for k,v in stats_dict.items()]

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


schema = ErrorEnabledSchema(Query, Mutation)


def get_graphql_app(rank: GravityRank):
    def get_meritrank_instance():
        return CustomContext(rank)

    async def get_context(custom_context=Depends(get_meritrank_instance)):
        return custom_context

    graphql_app = GraphQLRouter(schema, context_getter=get_context)
    LOGGER.info("Created GraphQL router")
    return graphql_app
