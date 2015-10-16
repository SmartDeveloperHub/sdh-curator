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
from abc import abstractmethod

from sdh.curator.actions import Action
from sdh.curator.actions.core.delivery import DeliveryRequest
from sdh.curator.actions.core.utils import CGraph
from rdflib.namespace import Namespace, RDF
from sdh.curator.actions.core.store import r
import logging

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.fragment')

CURATOR = Namespace('http://www.smartdeveloperhub.org/vocabulary/curator#')
AMQP = Namespace('http://www.smartdeveloperhub.org/vocabulary/amqp#')


class FragmentRequest(DeliveryRequest):
    def __init__(self):
        super(FragmentRequest, self).__init__()
        self.__pattern_graph = CGraph()

    def _extract_content(self):
        super(FragmentRequest, self)._extract_content()

        variables = set(self._graph.subjects(RDF.type, CURATOR.Variable))
        log.debug('{} variables involved in the the enrichment pattern to <{}>'.format(len(variables), self._target_resource))

        visited = set([])
        target_pattern = self._graph.predicate_objects(self._target_resource)
        for (pr, req_object) in target_pattern:
            if (req_object, RDF.type, CURATOR.Variable) in self._graph:
                visited.add(req_object)
                link_triple = (self._target_resource, pr, req_object)
                self.__pattern_graph.add(link_triple)
                log.debug('New pattern link: {}'.format(link_triple))

                self.__follow_variable(req_object, visited=visited)

        log.debug('Extracted pattern graph:\n{}'.format(self.__pattern_graph.serialize(format='turtle')))

    def __add_pattern_link(self, node, triple):
        is_variable = (node, RDF.type, CURATOR.Variable) in self._graph
        if is_variable:
            self.__pattern_graph.add(triple)
            log.debug('New pattern link: {}'.format(triple))
        return is_variable

    def __follow_variable(self, variable_node, visited=None):
        if visited is None:
            visited = set([])
        visited.add(variable_node)
        subject_pattern = self._graph.subject_predicates(variable_node)
        for (n, pr) in subject_pattern:
            if n not in visited and self.__add_pattern_link(n, (n, pr, variable_node)):
                self.__follow_variable(n, visited)

        object_pattern = self._graph.predicate_objects(variable_node)
        for (pr, n) in object_pattern:
            if self.__add_pattern_link(n, (variable_node, pr, n)):
                if n not in visited:
                    self.__follow_variable(n, visited)
            elif n != CURATOR.Variable:
                self.__pattern_graph.add((variable_node, pr, n))

    @property
    def pattern(self):
        return self.__pattern_graph


class FragmentAction(Action):
    @abstractmethod
    def request(self):
        pass

    def __init__(self, message):
        super(FragmentAction, self).__init__(message)

    def perform(self):
        super(FragmentAction, self).perform()
        log.debug('Storing request...')
        r.set('p', self.request().pattern.serialize(format='turtle'))

Action.register(FragmentAction)
