"""
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  This file is part of the Smart Developer Hub Project:
    http://www.smartdeveloperhub.org

  Center for Open Middleware
        http://www.centeropenmiddleware.com/
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Copyright (C) 2015 Center for Open Middleware.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at 

            http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
"""

from rdflib import Graph
import itertools
import networkx as nx
from networkx.algorithms.isomorphism import DiGraphMatcher

__author__ = 'Fernando Serena'


class CGraph(Graph):
    def objects(self, subject=None, predicate=None, card=None):
        objs_gen = super(CGraph, self).objects(subject, predicate)
        if card is None:
            return objs_gen

        objs_gen, gen_bak = itertools.tee(objs_gen)
        objs = list(objs_gen)

        if card == 1:
            if not (0 < len(objs) < 2):
                raise ValueError(len(objs))
            return objs.pop()

        return gen_bak

    def subjects(self, predicate=None, object=None, card=None):
        subs_gen = super(CGraph, self).subjects(predicate, object)
        if card is None:
            return subs_gen

        subs_gen, gen_bak = itertools.tee(subs_gen)
        subs = list(subs_gen)

        if card == 1:
            if not (0 < len(subs) < 2):
                raise ValueError(len(subs))
            return subs.pop()

        return gen_bak


class GraphPattern(set):
    def __init__(self, s=()):
        super(GraphPattern, self).__init__(s)

    @property
    def gp(self):
        return self

    @property
    def wire(self):
        g = nx.DiGraph()
        for tp in self:
            (s, p, o) = tuple(tp.split(' '))
            g.add_nodes_from([s, p, o])
            g.add_edge(s, o, link=p)
            if not o.startswith('?'):
                g.add_node(o, literal=True)

        return g

    def __eq__(self, other):
        my_wire = self.wire
        others_wire = other.wire

        def __node_match(n1, n2):
            return n1 == n2

        def __edge_match(e1, e2):
            return e1 == e2

        matcher = DiGraphMatcher(my_wire, others_wire, node_match=__node_match, edge_match=__edge_match)
        return matcher.is_isomorphic()


