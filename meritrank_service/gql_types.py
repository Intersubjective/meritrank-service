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
    edges: list[Edge]
    users: list[NodeScore]
    beacons: list[NodeScore]
    comments: list[NodeScore]
