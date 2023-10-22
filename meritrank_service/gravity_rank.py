import asyncio

from cachetools import TTLCache, cached
from meritrank_python.lazy import LazyMeritRank
from meritrank_python.rank import NodeId

from meritrank_service.gql_types import Edge
import networkx as nx

top_beacons_cache = TTLCache(maxsize=1, ttl=3600)


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

    def get_edges_for_node(self, node):
        return [Edge(src=e[0], dest=e[1], weight=e[2]) for e in self.get_node_edges(node)]

    def filter_node_by_type(self, ego, node):
        users = {}
        beacons = {}
        comments = {}
        score = self.get_node_score(ego, node)
        match node[0]:
            case 'U':
                users[node] = score
            case 'C':
                comments[node] = score
            case 'B':
                beacons[node] = score
            case _:
                self.logger.warning(f"Unknown node type: {node}")
        return users, beacons, comments

    def gravity_graph_filtered(self, *args, **kwargs):
        # This function filters leaf comments from the gravity graph.
        # The key assumption is that transitive edges in the algorithm are always consecutive, e.g.  [(U->C), (C->U)]

        edges, users, beacons, comments = self.gravity_graph(*args, **kwargs)

        # Initialize required variables
        transitive_pairs = set()
        result_edges = []
        skip_next = False
        comments_to_keep = set()

        # Iterate over the edges
        for i, edge in enumerate(edges):

            # If the skip_next flag is set, skip this iteration and reset the flag
            if skip_next:
                skip_next = False
                continue

            # Check if the edge is directed towards a comment, and if it forms a transitive pair
            is_comment_edge = edge.dest.startswith("C")
            if is_comment_edge:
                is_transitive_pair = i < len(edges) - 1 and edge.dest == edges[i + 1].src
                # If the edge is the last one and not part of a transitive pair, or doesn't form a transitive pair, skip it
                if (i == len(edges) - 1) or not is_transitive_pair:
                    continue

                # If this transitive pair has already been encountered, skip it and the next edge
                if is_transitive_pair and (pair := (edge.src, edges[i + 1].dest)) in transitive_pairs:
                    skip_next = True
                    continue

                elif is_transitive_pair:
                    transitive_pairs.add(pair)
                    comments_to_keep.add(edge.dest)

            # If an edge passes all the checks, add it to the result_edges list
            result_edges.append(edge)

        # Filter the comments dictionary to only include those in the comments_to_keep set
        comments = {key: comments[key] for key in comments_to_keep & comments.keys()}

        return result_edges, users, beacons, comments

    def gravity_graph(self, ego: str, focus_stack: list[str],
                      min_abs_score: float = None,
                      positive_only: bool = True,
                      max_recurse_depth: int = 2):
        """
        In Gravity social network, prefixes in the node names determine the type of the node.
        The prefixes are:
        "U" - user
        "B" - beacon
        "C" - comment
        The basic idea is to only include the following categories of the nodes in the graph:
        users,
        beacons,
        comment that lead to some other user.
        Basically, this means "everything except terminal/leaf/dead-end comments"
        The graph is returned as a list of edges, and a list of nodes.
        :param ego: ego to get the graph for
        :param focus_stack: stack of focus node to get the graph around. Start with e.g. [your_node]
        :param positive_only: only include nodes with positive scores
        :param min_abs_score: minimum absolute score of nodes to include in the graph
        :param max_recurse_depth: how deep to recurse into the graph
        :return: (List[Edge], List[NodeScore])
        """

        edges = []
        users, beacons, comments = self.filter_node_by_type(ego, focus_stack[-1])

        if len(focus_stack) > max_recurse_depth:
            return edges, users, beacons, comments

        for edge in self.get_edges_for_node(focus_stack[-1]):
            dest_score = self.get_node_score(ego, edge.dest)
            if (
                    (min_abs_score is not None and dest_score < min_abs_score) or
                    (positive_only and dest_score <= 0.0) or
                    (edge.dest == ego) or
                    (len(focus_stack) >= 2 and edge.dest == focus_stack[-2])  # do not explore back edges
            ):
                continue

            e, u, b, c = self.gravity_graph(ego, focus_stack + [edge.dest],
                                            min_abs_score=min_abs_score,
                                            positive_only=positive_only,
                                            max_recurse_depth=max_recurse_depth)
            if u or b or c:
                edges.append(edge)
            edges.extend(e)
            users.update(u)
            beacons.update(b)
            comments.update(c)

        return edges, users, beacons, comments

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
