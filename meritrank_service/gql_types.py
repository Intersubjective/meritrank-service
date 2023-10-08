from typing import Optional

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
    edges: Optional[list[Edge]] = None
    users: Optional[list[NodeScore]] = None
    beacons: Optional[list[NodeScore]] = None
    comments: Optional[list[NodeScore]] = None
