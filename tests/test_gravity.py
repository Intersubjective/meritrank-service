import pytest

import networkx as nx
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
            "CU1": {"weight": 1.0},
            # Edge to incorrect comment without author
            "CU000": {"weight": 1.0},
        },
        # User 1's comment
        "CU1": {
            "U1": {"weight": 1.0}
        },
        "U2": {
            "B2": {"weight": 1.0},
            "U1": {"weight": 1.0},
            "B33": {"weight": -1.0},

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
    # Just a smoke test
    g = GravityRank(graph=simple_gravity_graph)
    result = g.gravity_graph("U1", "U1", limit=10)
    combined_result = {(e.src + e.dest): e.weight for e in result[0]}
    assert "U1" + "CU000" not in combined_result
    assert len(result[0]) == 2
    result = g.gravity_graph("U1", "U2", limit=10)
    assert len(result[0]) == 3
    result = g.gravity_graph("U1", "U3", limit=10)
    assert 'C3' not in result[1]

def test_omit_zero_edges(simple_gravity_graph):
    zero = "U00000"
    graph = dict(simple_gravity_graph)
    graph.update({
       zero: {"U1":{"weight": 1.0}},
    })
    graph["U1"][zero] =  {"weight": 1.0}
    graph["U2"][zero] =  {"weight": 1.0}

    g = GravityRank(graph=graph, zero_node=zero)
    result = g.gravity_graph("U1", "U2", limit=1)
    assert zero not in result[1]


def test_global_ranks(simple_gravity_graph):
    # Just a smoke test
    g = GravityRank(graph=simple_gravity_graph)
    result = g.get_top_beacons_global()
    print(result)


def test_users_stats(simple_gravity_graph):
    g = GravityRank(graph=simple_gravity_graph)
