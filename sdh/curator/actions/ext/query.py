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
from sdh.curator.actions.core.fragment import FragmentRequest, FragmentAction, FragmentResponse, FragmentSink
from sdh.curator.actions.core import CURATOR, TYPES, RDF, XSD, FOAF
from sdh.curator.actions.core.utils import CGraph
from rdflib import BNode, URIRef, Literal, RDFS
from sdh.curator.store import r
from sdh.curator.actions.core.delivery import CURATOR_UUID
from sdh.curator.daemons.fragment import plugins, AGORA
from sdh.curator.store.triples import graph as triples

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.query')


def __process_fragment_triple(sink, (c, s, p, o), graph):
    pass
    # target = sink.target_resource
    # links = dict(map(lambda (l, v): (v, l), sink.target_links))
    # var_candidate = list(graph.objects(c, AGORA.subject))[0]
    # if (var_candidate, RDF.type, AGORA.Variable) in graph:
    #     var_label = str(list(graph.objects(var_candidate, RDFS.label))[0])
    #     if var_label in links:
    #         link = links[var_label]
    #         if not sink.is_link_set(link):
    #             triples.get_context('#enrichment').add((target, link, s))
    #             sink.set_link(links[var_label])
    #             print u'{} {} {} .'.format(target.n3(), link.n3(graph.namespace_manager),
    #                                        s.n3())


# plugins.append(__process_fragment_triple)


class QueryRequest(FragmentRequest):
    def __init__(self):
        super(QueryRequest, self).__init__()
        self._target_resource = None
        self._target_links = set([])

    def _extract_content(self):
        super(QueryRequest, self)._extract_content()


class QueryAction(FragmentAction):
    def __init__(self, message):
        self.__request = QueryRequest()
        self.__sink = QuerySink()
        super(QueryAction, self).__init__(message)

    @property
    def sink(self):
        return self.__sink

    @classmethod
    def response_class(cls):
        return QueryResponse

    @property
    def request(self):
        return self.__request

    def submit(self):
        super(QueryAction, self).submit()


class QuerySink(FragmentSink):
    def __init__(self):
        super(QuerySink, self).__init__()

    def _save(self, action):
        super(QuerySink, self)._save(action)

    def _load(self):
        super(QuerySink, self)._load()


class QueryResponse(FragmentResponse):
    def __init__(self, rid):
        self.__sink = QuerySink()
        self.__sink.load(rid)
        self.__v_index = 0
        super(QueryResponse, self).__init__(rid)

    @property
    def sink(self):
        return self.__sink

    def build(self):
        def __bound_literals(x):
            import re
            new_x = re.sub('".*"', lambda y: '?v{}'.format(self.__v_index), x)
            self.__v_index += 1
            return new_x

        gp = self.sink.gp
        self.__v_index = len(gp)
        log.debug('Building a query result for request number {}'.format(self._request_id))
        construct = map(lambda x: __bound_literals(x), gp)
        construct_gp = ' . '.join(construct)
        where = set.union(set(construct), gp)
        where_gp = ' . '.join(where)
        query = """CONSTRUCT { %s } WHERE { %s }""" % (construct_gp, where_gp)
        res_graph = self.query(query)
        graph = CGraph()
        for t in res_graph:
            graph.add(t)
        resp_node = BNode('#response')
        graph.add((resp_node, RDF.type, CURATOR.QueryResponse))
        graph.add((resp_node, CURATOR.messageId, Literal(self.sink.message_id, datatype=TYPES.UUID)))
        graph.add((resp_node, CURATOR.responseTo, Literal(self.sink.message_id, datatype=TYPES.UUID)))
        graph.add((resp_node, CURATOR.submittedOn, Literal(self.sink.submitted_on, datatype=XSD.dateTime)))
        curator_node = BNode('#curator')
        graph.add((resp_node, CURATOR.submittedBy, curator_node))
        graph.add((curator_node, RDF.type, FOAF.Agent))
        graph.add((curator_node, FOAF.agentId, CURATOR_UUID))
        return graph.serialize(format='turtle')
