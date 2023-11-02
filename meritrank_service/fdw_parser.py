from dataclasses import dataclass

from meritrank_python.lazy import LazyMeritRank

from meritrank_service.fdw_nng import NNGListener

SRC = 'src'
DEST = 'dest'
WEIGHT = 'weight'

FIELDS_ORDER = {SRC: 0, DEST: 1, WEIGHT: 2}


def sort_quals(quals):
    return sorted(quals, key=lambda x: FIELDS_ORDER.get(x.field, 10000))


@dataclass
class Qual:
    field: str
    operator: str
    value: str


def create_fdw_listener(meritrank, pg_fdw_listen_url):
    proc = FdwProcessor(meritrank)
    listener = NNGListener(proc.process_query, listen_url=pg_fdw_listen_url)
    return listener.start_listener()


class FdwProcessor:
    def __init__(self, meritrank: LazyMeritRank):
        self.mr = meritrank

    def process_query(self, quals, columns):
        print ("QUALS", quals)
        ego = None
        dest = None
        for q in sort_quals(Qual(*a) for a in quals):
            match q:
                case Qual('src', "=", value):
                    ego = value
                case Qual('dest', "=", value):
                    dest = value
                case _:
                    raise ValueError(f"Unknown field: {q.field}")
        if ego is None:
            raise ValueError("No source node specified")
        if dest is not None:
            return [(ego, dest, self.mr.get_node_score(ego, dest)), ]
        else:
            return [(ego, d, w) for d, w in self.mr.get_ranks(ego).items()]
