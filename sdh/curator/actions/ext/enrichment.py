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
import uuid
from datetime import datetime
import base64

from agora.client.agora import AGORA

from sdh.curator.actions.core.fragment import FragmentRequest, FragmentAction, FragmentResponse, FragmentSink
from sdh.curator.actions.core import CURATOR, TYPES, RDF, XSD, FOAF
from sdh.curator.actions.core.utils import CGraph
from rdflib import BNode, Literal, URIRef, RDFS
from sdh.curator.store import r
from sdh.curator.actions.core.delivery import CURATOR_UUID
from sdh.curator.daemons.fragment import FragmentPlugin
from sdh.curator.store.triples import cache
import shortuuid

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.enrichment')


def get_fragment_enrichments(fid):
    return [EnrichmentData(eid) for eid in r.smembers('fragments:{}:enrichments'.format(fid))]


def generate_enrichment_hash(target, links):
    links = '|'.join(sorted([str(pr) for (pr, _) in links]))
    eid = base64.b64encode('~'.join([target, links]))
    return eid


def register_enrichment(pipe, fid, target, links):
    e_hash = generate_enrichment_hash(target, links)
    if not r.sismember('enrichments', e_hash):
        eid = shortuuid.uuid()
        enrichment_data = EnrichmentData(eid, fid, target, links)
        enrichment_data.save(pipe)
        pipe.sadd('enrichments', e_hash)
        pipe.set('map:enrichments:{}'.format(e_hash), eid)
    else:
        eid = r.get('map:enrichments:{}'.format(e_hash))
    return eid


class EnrichmentData(object):
    def __init__(self, eid, fid=None, target=None, links=None):
        if eid is None:
            raise ValueError('Cannot create an enrichment data object without an identifier')

        self.links = links
        self.target = target
        self.fragment_id = fid
        self.enrichment_id = eid
        self._enrichment_key = 'enrichments:{}'.format(self.enrichment_id)

        if not any([fid, target, links]):
            self.load()

    def save(self, pipe):
        pipe.hset('{}'.format(self._enrichment_key), 'target', self.target)
        pipe.hset('{}'.format(self._enrichment_key), 'fragment_id', self.fragment_id)
        pipe.sadd('fragments:{}:enrichments'.format(self.fragment_id), self.enrichment_id)
        pipe.sadd('{}:links'.format(self._enrichment_key), *self.links)
        pipe.hmset('{}:links:status'.format(self._enrichment_key),
                   dict((pr, False) for (pr, _) in self.links))

    def load(self):
        dict_fields = r.hgetall(self._enrichment_key)
        self.target = URIRef(dict_fields.get('target', None))
        self.fragment_id = dict_fields.get('fragment_id', None)
        self.links = map(lambda (link, v): (URIRef(link), v), [eval(pair_str) for pair_str in
                                                               r.smembers('{}:links'.format(
                                                                   self._enrichment_key))])

    def set_link(self, link):
        with r.pipeline(transaction=True) as p:
            p.multi()
            p.hset('{}:links:status'.format(self._enrichment_key), str(link), True)
            p.execute()

    @property
    def completed(self):
        return all([eval(value) for value in r.hgetall('{}:links:status'.format(self._enrichment_key)).values()])


class EnrichmentPlugin(FragmentPlugin):
    @property
    def sink_class(self):
        return EnrichmentSink

    def sink_aware(self):
        return False

    def consume(self, fid, (c, s, p, o), graph, *args):
        enrichments = get_fragment_enrichments(fid)
        for e in enrichments:
            var_candidate = list(graph.objects(c, AGORA.subject))[0]
            if (var_candidate, RDF.type, AGORA.Variable) in graph:
                target = e.target
                links = dict(map(lambda (l, v): (v, l), e.links))
                var_label = str(list(graph.objects(var_candidate, RDFS.label))[0])
                if var_label in links:
                    link = links[var_label]
                    if (target, link, s) not in cache.get_context('#enrichment'):
                        e.set_link(link)
                        cache.get_context('#enrichment').add((target, link, s))
                        print u'{} {} {} .'.format(target.n3(), link.n3(graph.namespace_manager), s.n3())

    def complete(self, fid, *args):
        # TODO: check if all links are set
        pass


FragmentPlugin.register(EnrichmentPlugin)


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
            raise SyntaxError('Invalid enrichment request')

        request_fields = q_res.pop()
        if not all(request_fields):
            raise ValueError('Missing fields for enrichment request')
        if request_fields[0] != self._request_node:
            raise SyntaxError('Request node does not match')

        (self._target_resource,) = request_fields[1:]

        log.debug("""Parsed attributes of an enrichment request:
                    -target resource: {}""".format(self._target_resource))

        target_pattern = self._graph.predicate_objects(self._target_resource)
        for (pr, req_object) in target_pattern:
            if (req_object, RDF.type, CURATOR.Variable) in self._graph:
                self._target_links.add((pr, req_object))
        enrich_properties = set([pr for (pr, _) in self._target_links])
        if not enrich_properties:
            raise ValueError('There is nothing to enrich')
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
        try:
            super(EnrichmentAction, self).submit()
        except Exception as e:
            log.debug('Bad request: {}'.format(e.message))
            self._reply_failure(e.message)


class EnrichmentSink(FragmentSink):
    def _remove(self, pipe):
        pipe.srem('enrichments', self._request_id)
        super(FragmentSink, self)._remove(pipe)

    def __init__(self):
        super(EnrichmentSink, self).__init__()
        self.__target_links = None
        self.__target_resource = None
        self._enrichment_id = None
        self._enrichment_data = None

    def _save(self, action):
        super(EnrichmentSink, self)._save(action)
        variable_links = [(str(pr), self.map(self._variables_dict[v])) for (pr, v) in action.request.target_links]
        enrichment_id = register_enrichment(self._pipe, self._fragment_id, action.request.target_resource,
                                            variable_links)
        self._pipe.hset('{}'.format(self._request_key), 'enrichment_id', enrichment_id)
        self._dict_fields['enrichment_id'] = enrichment_id

    def _load(self):
        super(EnrichmentSink, self)._load()

    @property
    def enrichment_data(self):
        if self._enrichment_data is None:
            self._enrichment_data = EnrichmentData(self.enrichment_id)
        return self._enrichment_data

    @property
    def backed(self):
        return self.fragment_updated_on is not None and EnrichmentData(
            self.enrichment_id).completed


class EnrichmentResponse(FragmentResponse):
    def __init__(self, rid):
        self.__sink = EnrichmentSink()
        self.__sink.load(rid)
        super(EnrichmentResponse, self).__init__(rid)

    @property
    def sink(self):
        return self.__sink

    def _build(self):
        log.debug('Building a response to request number {}'.format(self._request_id))
        graph = CGraph()
        resp_node = BNode('#response')
        graph.add((resp_node, RDF.type, CURATOR.EnrichmentResponse))
        graph.add((resp_node, CURATOR.messageId, Literal(str(uuid.uuid4()), datatype=TYPES.UUID)))
        graph.add((resp_node, CURATOR.responseTo, Literal(self.sink.message_id, datatype=TYPES.UUID)))
        graph.add((resp_node, CURATOR.responseNumber, Literal("1", datatype=XSD.unsignedLong)))
        graph.add((resp_node, CURATOR.targetResource, self.sink.enrichment_data.target))
        graph.add((resp_node, CURATOR.submittedOn, Literal(datetime.now())))
        curator_node = BNode('#curator')
        graph.add((resp_node, CURATOR.submittedBy, curator_node))
        graph.add((curator_node, RDF.type, FOAF.Agent))
        graph.add((curator_node, CURATOR.agentId, CURATOR_UUID))
        addition_node = BNode('#addition')
        graph.add((resp_node, CURATOR.additionTarget, addition_node))
        graph.add((addition_node, RDF.type, CURATOR.Variable))
        for link, v in self.sink.enrichment_data.links:
            trs = self.graph().triples((self.sink.enrichment_data.target, link, None))
            for (_, _, o) in trs:
                graph.add((addition_node, link, o))
        yield graph.serialize(format='turtle'), {}
