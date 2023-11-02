import pytest
from meritrank_python.lazy import LazyMeritRank

from meritrank_service.fdw_parser import FdwProcessor


@pytest.fixture
def meritrank():
    mr = LazyMeritRank()
    mr.add_edge("a", "b")
    return mr


@pytest.fixture
def fdw_parser(meritrank):
    return FdwProcessor(meritrank)


def test_init(meritrank):
    fdw_parser = FdwProcessor(meritrank)
    assert fdw_parser.mr is meritrank


def test_process_query(fdw_parser, meritrank):
    quals = [
        ('src', '=', 'a'),
        ('dest', '=', 'b'),
    ]
    columns = ['src', 'dest', 'weight']
    ego = 'a'
    dest = 'b'
    expected_result = [(ego, dest, meritrank.get_node_score(ego, dest)), ]
    result = fdw_parser.process_query(quals, columns)
    assert result == expected_result


def test_process_query_no_dest(fdw_parser, meritrank):
    quals = [
        ('src', '=', 'a'),
    ]
    columns = ['src', 'dest', 'weight']
    ego = 'a'
    expected_result = [(ego, d, w) for d, w in meritrank.get_ranks(ego).items()]
    result = fdw_parser.process_query(quals, columns)
    assert result == expected_result


def test_process_query_unknown_field(fdw_parser):
    quals = [
        ('src', '=', 'a'),
        ('dest', '=', 'b'),
        ('unknown', '=', 'c'),
    ]
    columns = ['src', 'dest']
    with pytest.raises(ValueError) as excinfo:
        fdw_parser.process_query(quals, columns)
    assert str(excinfo.value) == 'Unknown field: unknown'
