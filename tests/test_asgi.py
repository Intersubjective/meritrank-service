from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from meritrank_service.asgi import create_meritrank_app
from meritrank_service.rest import Edge, MeritRankRestRoutes


@pytest.fixture()
def mrank():
    return Mock()


@pytest.fixture()
def rank_routes(mrank):
    return MeritRankRestRoutes(mrank)


@pytest.fixture()
def client(rank_routes):
    app = FastAPI()
    app.include_router(rank_routes.router)
    return TestClient(app=app)


def test_complete_init_with_default_values():
    assert TestClient(app=create_meritrank_app()).get(
        "/edge/0/1").status_code == 200


def test_get_node_score(mrank, rank_routes, client):
    result = 0.999
    mrank.get_node_score = lambda *_: result
    response = client.get("/node_score/0/1")
    assert response.status_code == 200
    assert response.json() == {'ego': '0', 'node': '1', 'score': 0.999}


def test_put_edge(mrank, rank_routes, client):
    e = Edge(src='a', dest='b', weight=1.0)
    response = client.put("/edge", data=e.json())
    assert response.status_code == 200
    mrank.add_edge.assert_called_once_with('a', 'b', 1.0)


def test_get_edge(mrank, rank_routes, client):
    mrank.get_node_edges = lambda _: [('a', 'b', 1.0)]
    response = client.get("/node_edges/0")
    assert response.status_code == 200
    assert response.json() == [Edge(src='a', dest='b', weight=1.0).dict()]
