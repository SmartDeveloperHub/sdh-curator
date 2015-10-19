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
import logging

from abc import abstractproperty, ABCMeta, abstractmethod
from rdflib import Literal
from sdh.curator.actions.core.delivery import DeliveryRequest, DeliveryAction
from sdh.curator.actions.core.utils import CGraph
from rdflib.namespace import Namespace, RDF
from sdh.curator.collect import agora_client

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.fragment')

CURATOR = Namespace('http://www.smartdeveloperhub.org/vocabulary/curator#')
AMQP = Namespace('http://www.smartdeveloperhub.org/vocabulary/amqp#')


class FragmentRequest(DeliveryRequest):
    def __init__(self):
        super(FragmentRequest, self).__init__()
        self.__pattern_graph = CGraph()
        self.__pattern_graph.bind('curator', CURATOR)
        prefixes = agora_client.prefixes
        for p in prefixes:
            self.__pattern_graph.bind(p, prefixes[p])

    def _extract_content(self):
        super(FragmentRequest, self)._extract_content()

        variables = set(self._graph.subjects(RDF.type, CURATOR.Variable))
        log.debug(
            'Found {} variables in the the fragment pattern'.format(len(variables)))

        visited = set([])
        for v in variables:
            self.__pattern_graph.add((v, RDF.type, CURATOR.Variable))
            self._follow_variable(v, visited=visited)
        log.debug('Extracted (fragment) pattern graph:\n{}'.format(self.__pattern_graph.serialize(format='turtle')))

    def _add_pattern_link(self, node, triple):
        type_triple = (node, RDF.type, CURATOR.Variable)
        condition = type_triple in self._graph
        if condition:
            self.__pattern_graph.add(type_triple)
            if triple not in self.__pattern_graph:
                self.__pattern_graph.add(triple)
                condition = True
                log.debug('New pattern link: {}'.format(triple))
        return condition

    def _follow_variable(self, variable_node, visited=None):
        if visited is None:
            visited = set([])
        visited.add(variable_node)
        subject_pattern = self._graph.subject_predicates(variable_node)
        for (n, pr) in subject_pattern:
            if self._add_pattern_link(n, (n, pr, variable_node)) and n not in visited:
                self._follow_variable(n, visited)

        object_pattern = self._graph.predicate_objects(variable_node)
        for (pr, n) in object_pattern:
            if self._add_pattern_link(n, (variable_node, pr, n)):
                if n not in visited:
                    self._follow_variable(n, visited)
            elif n != CURATOR.Variable:
                self.__pattern_graph.add((variable_node, pr, n))

    @property
    def pattern(self):
        return self.__pattern_graph


class FragmentAction(DeliveryAction):
    __metaclass__ = ABCMeta

    def __init__(self, message):
        super(FragmentAction, self).__init__(message)
        self._graph_pattern = set([])
        self._variables_dict = {}

    @abstractproperty
    def request(self):
        pass

    def _n3(self, elm):
        return elm.n3(self.request.pattern.namespace_manager)

    @abstractmethod
    def build_graph_pattern(self, v_labels=None):
        variables = set(self.request.pattern.subjects(RDF.type, CURATOR.Variable))
        if v_labels is not None:
            self._variables_dict = v_labels.copy()
        for i, v in enumerate(variables):
            self._variables_dict[v] = '?v{}'.format(i)
        for v in self._variables_dict.keys():
            v_in = self.request.pattern.subject_predicates(v)
            for (s, pr) in v_in:
                s_part = self._variables_dict[s] if s in self._variables_dict else self._n3(s)
                self._graph_pattern.add(u'{} {} {}'.format(s_part, self._n3(pr), self._variables_dict[v]))
            v_out = self.request.pattern.predicate_objects(v)
            for (pr, o) in [_ for _ in v_out if _[1] != CURATOR.Variable]:
                o_part = self._variables_dict[o] if o in self._variables_dict else ('"{}"'.format(o) if isinstance(o, Literal) else self._n3(o))
                p_part = self._n3(pr) if pr != RDF.type else 'a'
                self._graph_pattern.add(u'{} {} {}'.format(self._variables_dict[v], p_part, o_part))

    def submit(self):
        super(FragmentAction, self).submit()
        graph_pattern_str = '{ %s }' % ' . '.join(self._graph_pattern)
        log.debug(
            '{} triple patterns identified:\n{}'.format(len(self._graph_pattern),
                                                        graph_pattern_str.replace(' . ', ' .\n')))

    @abstractmethod
    def _persist(self):
        super(FragmentAction, self)._persist()
        self.build_graph_pattern()
        self._pipe.sadd('fragments', self._request_id)
        self._pipe.sadd('{}:gp'.format(self._request_key), *self._graph_pattern)

    @property
    def graph_pattern(self):
        return self._graph_pattern
