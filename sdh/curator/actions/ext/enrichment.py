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
from rdflib import BNode, URIRef, Literal
from sdh.curator.store import r
from sdh.curator.actions.core.delivery import CURATOR_UUID

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.enrichment')


class EnrichmentRequest(FragmentRequest):
    def __init__(self):
        super(EnrichmentRequest, self).__init__()
        self._target_resource = None
        self._target_links = set([])

    def _extract_content(self):
        super(EnrichmentRequest, self)._extract_content()

        q_res = self._graph.query("""SELECT ?node ?t WHERE {
                                        ?node a curator:EnrichmentRequest;
                                              curator:targetResource ?t
                                    }""")

        q_res = list(q_res)
        if len(q_res) != 1:
            raise SyntaxError('Invalid fragment request')

        request_fields = q_res.pop()
        if not all(request_fields):
            raise ValueError('Missing fields for fragment request')
        if request_fields[0] != self._request_node:
            raise SyntaxError('Request node does not match')

        (self._target_resource,) = request_fields[1:]

        log.debug("""Parsed attributes of an enrichment action request:
                    -target resource: {}""".format(self._target_resource))

        target_pattern = self._graph.predicate_objects(self._target_resource)
        for (pr, req_object) in target_pattern:
            if (req_object, RDF.type, CURATOR.Variable) in self._graph:
                self._target_links.add((pr, req_object))
        enrich_properties = set([pr for (pr, _) in self._target_links])
        log.debug(
            '<{}> is requested to be enriched with values for the following properties:\n{}'.format(
                self._target_resource,
                '\n'.join(enrich_properties)))

    @property
    def target_resource(self):
        return self._target_resource

    @property
    def target_links(self):
        return self._target_links.copy()


class EnrichmentAction(FragmentAction):
    def __init__(self, message):
        self.__request = EnrichmentRequest()
        self.__sink = EnrichmentSink()
        super(EnrichmentAction, self).__init__(message)

    @property
    def sink(self):
        return self.__sink

    @classmethod
    def response_class(cls):
        return EnrichmentResponse

    @property
    def request(self):
        return self.__request

    def submit(self):
        super(EnrichmentAction, self).submit()


class EnrichmentSink(FragmentSink):
    def __init__(self):
        super(EnrichmentSink, self).__init__()
        self.__target_links = None
        self.__target_resource = None

    def _save(self, action):
        super(EnrichmentSink, self)._save(action)
        self._pipe.sadd('enrichments', self._request_id)
        self._pipe.set('{}:enrich'.format(self._request_key), action.request.target_resource)
        variable_links = [(str(pr), self._variables_dict[v]) for (pr, v) in action.request.target_links]
        self._pipe.sadd('{}:enrich:links'.format(self._request_key), *variable_links)

    def _load(self):
        super(EnrichmentSink, self)._load()
        log.debug('Loading enrichment request data...')
        self.__target_links = [URIRef(eval(pair_str)[0]) for pair_str in
                               r.smembers('{}:enrich:links'.format(self._request_key))]
        self.__target_resource = URIRef(r.get('{}:enrich'.format(self._request_key)))

    @property
    def target_links(self):
        return self.__target_links

    @property
    def target_resource(self):
        return self.__target_resource


class EnrichmentResponse(FragmentResponse):
    def __init__(self, rid):
        self.__sink = EnrichmentSink()
        self.__sink.load(rid)
        super(EnrichmentResponse, self).__init__(rid)

    @property
    def sink(self):
        return self.__sink

    def build(self):
        graph = CGraph()
        resp_node = BNode('#response')
        graph.add((resp_node, RDF.type, CURATOR.EnrichmentResponse))
        graph.add((resp_node, CURATOR.messageId, Literal(self.sink.message_id, datatype=TYPES.UUID)))
        graph.add((resp_node, CURATOR.responseTo, Literal(self.sink.message_id, datatype=TYPES.UUID)))
        graph.add((resp_node, CURATOR.submittedOn, Literal(self.sink.submitted_on, datatype=XSD.dateTime)))
        curator_node = BNode('#curator')
        graph.add((resp_node, CURATOR.submittedBy, curator_node))
        graph.add((curator_node, RDF.type, FOAF.Agent))
        graph.add((curator_node, FOAF.agentId, CURATOR_UUID))
        addition_node = BNode('#addition')
        graph.add((resp_node, CURATOR.additionTarget, addition_node))
        graph.add((addition_node, RDF.type, CURATOR.Variable))
        for link in self.sink.target_links:
            triples = self.graph.triples((self.sink.target_resource, link, None))
            for (_, _, o) in triples:
                graph.add((addition_node, link, o))
        log.debug('Preparing to respond to request number {}'.format(self._request_id))
        return graph.serialize(format='turtle')
