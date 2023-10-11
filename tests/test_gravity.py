import pytest

from meritrank_service.gravity_rank import GravityRank


@pytest.fixture()
def simple_gravity_graph():
    return {
        "U1": {
            "B1": {"weight": 1.0},
            "B2": {"weight": 1.0},
            "C3": {"weight": 1.0},
            "C4": {"weight": 1.0},
            "U2": {"weight": 1.0},
        },
        "U2": {
            "B2": {"weight": 1.0},
            "U1": {"weight": 1.0},

        },
        "B1": {"U1": {"weight": 1.0}},
        "B2": {"U2": {"weight": 1.0}},
        "U3": {
            "C3": {"weight": 1.0},
            "C4": {"weight": 1.0},
            "B33": {"weight": 1.0},
        },
        "B33": {
            "U3": {"weight": 1.0},
        },
        "C3": {
            "U3": {"weight": 1.0},
        },
        "C4": {
            "U3": {"weight": 1.0},
        },


    }


def test_gravity_graph(simple_gravity_graph):
    g = GravityRank(graph=simple_gravity_graph)
    result = g.gravity_graph_filtered("U1", ["U1"])
    print (result)
