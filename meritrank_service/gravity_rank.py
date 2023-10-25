import asyncio

from cachetools import TTLCache, cached
from meritrank_python.lazy import LazyMeritRank
from meritrank_python.rank import NodeId

from meritrank_service.gql_types import Edge
import networkx as nx

top_beacons_cache = TTLCache(maxsize=1, ttl=3600)


def out_degree_single(G, node):
    # Due to a bug/deficincy of Networkx, it assumes str-identified nodes as interables
    maybe_iter = G.out_degree(node)
    if isinstance(maybe_iter, int):
        return maybe_iter
    else:
        if maybe_iter:
            return maybe_iter[0]
        else:
            return 0

def weight_fun(u, v, edge):
    w = edge['weight']
    if w > 0:
        return 1.0 / w
    return 10 ^ 20


def filter_dict_by_set(d, s):
    return {k: v for k, v in d.items() if k in s}


class GravityRank(LazyMeritRank):

    @cached(top_beacons_cache)
    def __get_top_beacons_global(self):
        reduced_graph = nx.DiGraph()
        for ego in self._IncrementalMeritRank__graph.nodes():
            if not ego.startswith("U"):
                continue
            for dest, score in self.get_ranks(ego).items():
                if ((dest.startswith("U") or dest.startswith("B"))
                        and (score > 0.0)
                        and (ego != dest)):
                    reduced_graph.add_edge(ego, dest, weight=score)

        top_nodes = nx.pagerank(reduced_graph)
        sorted_ranks = sorted(((k, v) for k, v in top_nodes.items() if k.startswith('B')), key=lambda x: x[1],
                              reverse=True)
        return sorted_ranks

    def get_top_beacons_global(self, limit=None, use_cache=True) -> dict[NodeId, float]:
        if not use_cache:
            top_beacons_cache.clear()

        return dict(self.__get_top_beacons_global()[:limit])

    def add_path_to_graph(self, G, ego, focus):
        ego_to_focus_path = nx.dijkstra_path(self._IncrementalMeritRank__graph, ego, focus, weight=weight_fun)
        path_edges = [(src, dest, self.get_edge(src, dest)) for src, dest in nx.utils.pairwise(ego_to_focus_path)]
        G.add_weighted_edges_from(path_edges)

    def remove_non_positive(self, ego, G):
        for node in list(G.nodes()):
            if self.get_node_score(ego, node) <= 0:
                G.remove_node(node)

    def remove_terminal_comments(self, G):
        for src, dest in list(G.edges()):
            if dest.startswith("C") and out_degree_single(G, dest) <= 1:
                if G.has_node(dest):
                    G.remove_node(dest)

    def remove_terminal_beacons(self, G):
        for src, dest in list(G.edges()):
            if dest.startswith("B") and out_degree_single(G, dest) <= 1:
                if G.has_node(dest):
                    G.remove_node(dest)

    def get_users_stats(self, ego):
        all_ranks = self.get_ranks(ego)
        users_stats = {}
        for node, score in all_ranks:
            if node.startswith("U"):
                users_stats[node] = score


    def remove_duplicate_transitive_comments(self, G):
        for node in list(G.nodes()):
            if node.startswith("U"):
                node_already_connected = False
                for src, dest in list(G.in_edges(node)):
                    if src.startswith("C"):
                        if node_already_connected:
                            G.remove_node(src)
                        else:
                            node_already_connected = True

    def gravity_graph(self, ego: str, focus: str,
                      positive_only: bool = True) -> tuple[list[Edge], dict[str, float]]:
        G = nx.ego_graph(self._IncrementalMeritRank__graph, focus, radius=2)

        if positive_only:
            self.remove_non_positive(ego, G)
        self.remove_terminal_comments(G)
        self.remove_terminal_beacons(G)
        self.remove_duplicate_transitive_comments(G)
        try:
            self.add_path_to_graph(G, ego, focus)
        except nx.exception.NetworkXNoPath:
            # No path found, so add just the focus node to show at least something
            G.add_node(focus)

        nodes_dict = {n: self.get_node_score(ego, n) for n in G.nodes()}
        edges = [Edge(src=src, dest=dest, weight=self.get_edge(src, dest)) for src, dest in G.edges()]

        return edges, nodes_dict

    async def warmup(self, wait_time=0):
        # Maybe wait a bit for other services to start up
        await asyncio.sleep(wait_time)
        self.logger.info(f"Starting ego warmup")
        all_egos = [ego for ego in self._IncrementalMeritRank__graph.nodes() if ego.startswith("U")]
        for ego in all_egos:
            self.calculate(ego)
            # Just pass the control to the reactor for a brief moment
            await asyncio.sleep(0)
        self.logger.info(f"Starting warmup for global beacons score")
        self.__get_top_beacons_global()
