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
from datetime import datetime
import uuid

from sdh.curator.actions.core.fragment import FragmentRequest, FragmentAction, FragmentResponse, FragmentSink
from sdh.curator.actions.core import CURATOR, TYPES, RDF, XSD, FOAF
from sdh.curator.actions.core.utils import CGraph
from rdflib import BNode, Literal
from sdh.curator.actions.core.delivery import CURATOR_UUID
from sdh.curator.messaging.reply import reply
from sdh.curator.daemons.fragment import FragmentPlugin

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.query')

class QueryPlugin(FragmentPlugin):
    @property
    def sink_class(self):
        return QuerySink

    def consume(self, fid, (c, s, p, o), graph, *args):
        pass

    def complete(self, fid, *args):
        pass


FragmentPlugin.register(QueryPlugin)

class QueryRequest(FragmentRequest):
    def __init__(self):
        super(QueryRequest, self).__init__()

    def _extract_content(self):
        super(QueryRequest, self)._extract_content()

        q_res = self._graph.query("""SELECT ?node WHERE {
                                        ?node a curator:QueryRequest .
                                    }""")

        q_res = list(q_res)
        if len(q_res) != 1:
            raise SyntaxError('Invalid query request')

        request_fields = q_res.pop()
        if not all(request_fields):
            raise ValueError('Missing fields for query request')
        if request_fields[0] != self._request_node:
            raise SyntaxError('Request node does not match')


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
        try:
            super(QueryAction, self).submit()
        except Exception as e:
            log.debug('Bad request: {}'.format(e.message))
            self._reply_failure(e.message)


class QuerySink(FragmentSink):
    def _remove(self, pipe):
        super(QuerySink, self)._remove(pipe)

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
        super(QueryResponse, self).__init__(rid)

    @property
    def sink(self):
        return self.__sink

    def build(self):
        fragment, _ = self.fragment()
        graph = CGraph()
        log.debug('Building a query result for request number {}'.format(self._request_id))
        for t in fragment:
            graph.add(t)
        resp_node = BNode('#response')
        graph.add((resp_node, RDF.type, CURATOR.QueryResponse))
        graph.add((resp_node, CURATOR.messageId, Literal(str(uuid.uuid4()), datatype=TYPES.UUID)))
        graph.add((resp_node, CURATOR.responseTo, Literal(self.sink.message_id, datatype=TYPES.UUID)))
        graph.add((resp_node, CURATOR.submittedOn, Literal(datetime.now(), datatype=XSD.dateTime)))
        curator_node = BNode('#curator')
        graph.add((resp_node, CURATOR.submittedBy, curator_node))
        graph.add((curator_node, RDF.type, FOAF.Agent))
        graph.add((curator_node, FOAF.agentId, CURATOR_UUID))
        yield graph.serialize(format='turtle'), {}
