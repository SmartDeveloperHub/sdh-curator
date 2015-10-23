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
from shortuuid import uuid

from abc import ABCMeta, abstractmethod
from rdflib import Literal, URIRef, XSD
from sdh.curator.actions.core.delivery import DeliveryRequest, DeliveryAction, DeliveryResponse, DeliverySink
from sdh.curator.actions.core.utils import CGraph
from sdh.curator.actions.core import CURATOR, RDF
from sdh.curator.daemons.fragment import agora_client
from sdh.curator.store import r
from sdh.curator.store.triples import graph as triples

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.fragment')


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

    def submit(self):
        super(FragmentAction, self).submit()


class FragmentSink(DeliverySink):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(FragmentSink, self).__init__()
        self._graph_pattern = set([])
        self._variables_dict = {}

    @staticmethod
    def _n3(action, elm):
        return elm.n3(action.request.pattern.namespace_manager)

    def _build_graph_pattern(self, action, v_labels=None):
        variables = set(action.request.pattern.subjects(RDF.type, CURATOR.Variable))
        if v_labels is not None:
            self._variables_dict = v_labels.copy()
        for i, v in enumerate(variables):
            self._variables_dict[v] = '?v{}'.format(i)
        for v in self._variables_dict.keys():
            v_in = action.request.pattern.subject_predicates(v)
            for (s, pr) in v_in:
                s_part = self._variables_dict[s] if s in self._variables_dict else self._n3(action, s)
                self._graph_pattern.add(u'{} {} {}'.format(s_part, self._n3(action, pr), self._variables_dict[v]))
            v_out = action.request.pattern.predicate_objects(v)
            for (pr, o) in [_ for _ in v_out if _[1] != CURATOR.Variable]:
                o_part = self._variables_dict[o] if o in self._variables_dict else (
                    '"{}"'.format(o) if isinstance(o, Literal) else self._n3(action, o))
                p_part = self._n3(action, pr) if pr != RDF.type else 'a'
                self._graph_pattern.add(u'{} {} {}'.format(self._variables_dict[v], p_part, o_part))

    @abstractmethod
    def _save(self, action):
        super(FragmentSink, self)._save(action)
        self._build_graph_pattern(action)
        fgm_id = str(uuid())
        self._pipe.sadd('fragments', fgm_id)
        self._pipe.sadd('fragments:{}:gp'.format(fgm_id), *self._graph_pattern)
        self._pipe.sadd('fragments:{}:requests'.format(fgm_id), self._request_id)
        self._pipe.hset('{}'.format(self._request_key), 'gp', fgm_id)

    @abstractmethod
    def _remove(self, pipe):
        fgm_id = r.hget('{}'.format(self._request_key), 'gp')
        pipe.srem('fragments:{}:requests'.format(fgm_id), self._request_id)
        super(FragmentSink, self)._remove(pipe)

    @abstractmethod
    def _load(self):
        super(FragmentSink, self)._load()
        fgm_id = self._dict_fields['gp']
        self._dict_fields['gp'] = r.smembers('fragments:{}'.format(fgm_id))

    @property
    def backed(self):
        return not r.sismember('fragments', self._request_id)

    @backed.setter
    def backed(self, value):
        def __pipe_set_func(f):
            with r.pipeline(transaction=True) as p:
                p.multi()
                p.__getattribute__(f)('fragments', self._request_id)
                p.execute()
        if value:
            __pipe_set_func('srem')
        else:
            __pipe_set_func('sadd')


class FragmentResponse(DeliveryResponse):
    __metaclass__ = ABCMeta

    def __init__(self, rid):
        super(FragmentResponse, self).__init__(rid)

    @abstractmethod
    def build(self):
        super(FragmentResponse, self).build()

    @staticmethod
    def query(query_object):
        return triples.query(query_object)

    @property
    def graph(self):
        return triples

    @property
    def fragment(self):
        def __transform(x):
            if x.startswith('"'):
                return Literal(x.replace('"', ''), datatype=XSD.string).n3(self.graph.namespace_manager)
            return x

        gp = [' '.join([__transform(part) for part in tp.split(' ')]) for tp in self.sink.gp]
        where_gp = ' . '.join(gp)
        query = """CONSTRUCT WHERE { %s }""" % where_gp
        return self.graph.query(query)






