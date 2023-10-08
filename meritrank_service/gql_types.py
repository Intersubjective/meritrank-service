from typing import List, Optional

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
    edges: List[Optional[Edge]]
    users: List[Optional[NodeScore]]
    beacons: List[Optional[NodeScore]]
    comments: List[Optional[NodeScore]]
