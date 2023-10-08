from typing import List

import strawberry


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


@strawberry.type
class GravityGraph:
    edges: List[Edge]
    users: List[NodeScore]
    beacons: List[NodeScore]
    comments: List[NodeScore]
