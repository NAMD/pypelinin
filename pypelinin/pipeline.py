# coding: utf-8

from itertools import product

from pygraph.classes.digraph import digraph as DiGraph
from pygraph.algorithms.cycles import find_cycle
from pygraph.readwrite.dot import write


class Job(object):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'Job({})'.format(repr(self.name))

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.name)


class Pipeline(object):
    def __init__(self, pipeline):
        self._original_graph = pipeline
        self._normalize()
        nodes = set()
        for key, value in self._graph:
            nodes.add(key)
            nodes.add(value)
        nodes.discard(None)
        self.jobs = tuple(nodes)
        self._define_starters()
        self._create_digraph()
        if not self._validate():
            raise ValueError('The pipeline graph have cycles or do not have a '
                             'starter job')

    def _normalize(self):
        new_graph = []
        for keys, values in self._original_graph.items():
            if type(keys) is Job:
                keys = [keys]
            if type(values) not in (tuple, list):
                values = [values]
            for key in keys:
                if not values:
                    new_graph.append((key, None))
                else:
                    for value in values:
                        new_graph.append((key, value))
        self._graph = new_graph

    def _define_starters(self):
        possible_starters = set()
        others = set()
        for key, value in self._graph:
            others.add(value)
            possible_starters.add(key)
        self.starters = tuple(possible_starters - others)

    def _create_digraph(self):
        digraph = DiGraph()
        digraph.add_nodes(self.jobs)
        for edge in self._graph:
            if edge[1] is not None:
                digraph.add_edge(edge)
        self._digraph = digraph

    def _validate(self):
        #TODO: A -> B, A -> C, B -> C
        if len(self.starters) == 0:
            return False
        if find_cycle(self._digraph):
            return False
        return True

    def to_dot(self):
        return write(self._digraph)
