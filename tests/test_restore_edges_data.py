from meritrank_service.postgres_edges_provider import get_edges_data


def test_get_data_from_db():
    url = "postgres://postgres:12345678@localhost:54321/postgres"
    get_edges_data(url)
