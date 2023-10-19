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
    result = g.gravity_graph_filtered("U1", ["U1"])
    combined_result = {(e.src + e.dest): e.weight for e in result[0]}
    assert "U1" + "CU000" not in combined_result


def test_global_ranks(simple_gravity_graph):
    # Just a smoke test
    g = GravityRank(graph=simple_gravity_graph)
    result = g.get_top_beacons_global()
    print(result)
