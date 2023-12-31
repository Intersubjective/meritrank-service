import asyncio

from meritrank_python.lazy import LazyMeritRank
from meritrank_python.rank import NodeId

from meritrank_service.gql_types import Edge
import networkx as nx


def in_degree_single(G, node):
    # Due to a bug/deficincy of Networkx, it assumes str-identified nodes as interables
    maybe_iter = G.in_degree(node)
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
    return None


def filter_dict_by_set(d, s):
    return {k: v for k, v in d.items() if k in s}


class GravityRank(LazyMeritRank):

    def get_top_beacons_global(self):
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

    def refresh_zero_opinion(self, zero_node, top_nodes_limit=100):
        # Zero out existing edges of the zero node, to avoid affecting
        # the global ranking calculation
        for _, dst, _ in self.get_node_edges(zero_node):
            self.add_edge(zero_node, dst, 0.0)
        top_nodes = self.get_top_beacons_global()
        for dst, score in top_nodes[:top_nodes_limit]:
            self.add_edge(zero_node, dst, score)

    def add_path_to_graph(self, G, ego, focus):
        if ego == focus:
            return
        ego_to_focus_path = nx.dijkstra_path(self._IncrementalMeritRank__graph, ego, focus, weight=weight_fun)
        ego_to_focus_path.append(None)

        edges = []
        for a, b, c in zip(ego_to_focus_path, ego_to_focus_path[1:], ego_to_focus_path[2:]):
            # merge transitive edges going through comments and beacons
            if c is None and not (a.startswith("C") or a.startswith("B")):
                new_edge = (a, b, self.get_edge(a, b))
            elif b.startswith("C") or b.startswith("B"):
                new_edge = (a, c, self.get_transitive_edge_weight(a, b, c))
            elif a.startswith("U"):
                new_edge = (a, b, self.get_edge(a, b))

            edges.append(new_edge)
        if len(ego_to_focus_path) == 2:
            # Add the final (and only)
            final_nodes = ego_to_focus_path[-2:]
            final_edge = (*final_nodes, self.get_edge(*final_nodes))
            edges.append(final_edge)
        G.add_weighted_edges_from(edges)

    def get_users_stats(self, ego) -> dict[str, (float, float)]:
        all_ranks = self.get_ranks(ego)
        users_stats = {}
        for node, score in all_ranks.items():
            if node.startswith("U") and score > 0.0:
                # Get both forward (ego->node), and reverse (node->ego) scores
                users_stats[node] = score, self.get_node_score(node, ego)

        return users_stats

    def remove_outgoing_edges_upto_limit(self, G, ego, focus, limit):
        neighbours = list(dest for src, dest in G.out_edges(focus))

        for dest in sorted(neighbours, key=lambda x: self.get_node_score(ego, x))[limit:]:
            G.remove_edge(focus, dest)
            G.remove_node(dest)

    def remove_self_edges(self, G):
        for src, dest in list(G.edges()):
            if src == dest:
                G.remove_edge(src, dest)

    def get_transitive_edge_weight(self, a, b, c):
        w_ab = self.get_edge(a, b)
        w_bc = self.get_edge(b, c)
        # TODO: proper handling of negative edges
        # Note that enemy of my enemy is not my friend.
        # Though, this is pretty irrelevant for our current case
        # where comments can't have outgoing negative edges.
        return w_ab * w_bc * (-1 if w_ab < 0 and w_bc < 0 else 1)

    def gravity_graph(self, ego: str, focus: str,
                      positive_only: bool = True,
                      limit: int | None = None
                      ) -> tuple[list[Edge], dict[str, float]]:
        G = nx.DiGraph()
        for a, b, _ in self.get_node_edges(focus):
            if b.startswith("U"):
                if positive_only and self.get_node_score(ego, b) <= 0:
                    continue
                # For direct user->user add all of them
                assert (self.get_edge(a, b) is not None)
                G.add_edge(a, b, weight=self.get_edge(a, b))
            elif b.startswith("C") or b.startswith("B"):
                # For connections user-> comment | beacon -> user,
                # convolve those into user->user
                for _, c, _ in self.get_node_edges(b):
                    if positive_only and self.get_node_score(ego, c) <= 0:
                        continue
                    if c == a:
                        # Don't include back edges
                        continue
                    if not c.startswith("U"):
                        # Only include edges to users
                        continue
                    w_ac = self.get_transitive_edge_weight(a, b, c)
                    G.add_edge(a, c, weight=w_ac)

        self.remove_outgoing_edges_upto_limit(G, ego, focus, limit or 3)

        try:
            self.add_path_to_graph(G, ego, focus)
        except nx.exception.NetworkXNoPath:
            # No path found, so add just the focus node to show at least something
            G.add_node(focus)

        self.remove_self_edges(G)

        nodes_dict = {n: self.get_node_score(ego, n) for n in G.nodes()}
        edges = [Edge(src=src, dest=dest, weight=G.get_edge_data(src, dest)['weight']) for src, dest in G.edges()]

        return edges, nodes_dict

    async def zero_opinion_heartbeat(self, zero_node, top_nodes_limit, refresh_period):
        self.logger.info(f"Starting zero opinion heartbeat")
        while True:
            self.logger.info(f"Refreshing zero opinion")
            self.refresh_zero_opinion(zero_node=zero_node, top_nodes_limit=top_nodes_limit)
            await asyncio.sleep(refresh_period)

    async def warmup(self, wait_time=0):
        # Maybe wait a bit for other services to start up
        await asyncio.sleep(wait_time)
        self.logger.info(f"Starting ego warmup")
        all_egos = [ego for ego in self._IncrementalMeritRank__graph.nodes() if ego.startswith("U")]
        for ego in all_egos:
            self.calculate(ego)
            # Just pass the control to the reactor for a brief moment
            await asyncio.sleep(0.01)
