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

from abc import ABCMeta, abstractmethod
from sdh.curator.actions.core.base import Request, Action, Response, Sink
from sdh.curator.actions.core.utils import CGraph
from rdflib import BNode, Literal
from sdh.curator.actions.core import RDF, CURATOR, FOAF, TYPES
from sdh.curator.messaging.reply import reply
from sdh.curator.store import r
from datetime import datetime
import base64

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.delivery')

CURATOR_UUID = Literal('00000000-0000-0000-0001-000000000002', datatype=TYPES.UUID)

_accepted_template = CGraph()
_accept_node = BNode('#accepted')
_curator_node = BNode('#curator')
_accepted_template.add((_accept_node, RDF.type, CURATOR.Accepted))
_accepted_template.add((_curator_node, RDF.type, FOAF.Agent))
_accepted_template.add((_accept_node, CURATOR.submittedBy, _curator_node))
_accepted_template.add((_accept_node, CURATOR.submittedBy, _curator_node))
_accepted_template.add(
    (_curator_node, CURATOR.agentId, CURATOR_UUID))
_accepted_template.bind('types', TYPES)
_accepted_template.bind('curator', CURATOR)
_accepted_template.bind('foaf', FOAF)


class DeliveryRequest(Request):
    def __init__(self):
        super(DeliveryRequest, self).__init__()

    def _extract_content(self):
        super(DeliveryRequest, self)._extract_content()

        q_res = self._graph.query("""SELECT ?node ?ex ?q ?rk ?h ?p ?v WHERE {
                                  ?node curator:replyTo [
                                        a curator:DeliveryChannel;
                                        amqp:exchangeName ?ex;
                                        amqp:queueName ?q;
                                        amqp:routingKey ?rk;
                                        amqp:broker [
                                           a amqp:Broker;
                                           amqp:host ?h;
                                           amqp:port ?p;
                                           amqp:virtualHost ?v
                                        ]
                                     ]
                                  } """)
        q_res = list(q_res)
        if len(q_res) != 1:
            raise SyntaxError('Invalid delivery request')

        request_fields = q_res.pop()

        if not any(request_fields):
            raise ValueError('Missing fields for delivery request')

        if request_fields[0] != self._request_node:
            raise SyntaxError('Request node does not match')

        delivery_data = {}

        (delivery_data['exchange'],
         delivery_data['queue'],
         delivery_data['routing_key'],
         delivery_data['host'],
         delivery_data['port'],
         delivery_data['vhost']) = request_fields[1:]
        log.debug("""Parsed attributes of a delivery action request:
                      -exchange name: {}
                      -queue name: {}
                      -routing key: {}
                      -host: {}
                      -port: {}
                      -virtual host: {}""".format(
            delivery_data['exchange'],
            delivery_data['queue'],
            delivery_data['routing_key'],
            delivery_data['host'], delivery_data['port'], delivery_data['vhost']))

        self._fields['delivery'] = delivery_data.copy()

    @property
    def broker(self):
        return {k: self._fields['delivery'][k] for k in ('host', 'port', 'vhost') if k in self._fields['delivery']}

    @property
    def channel(self):
        return {k: self._fields['delivery'][k] for k in ('exchange', 'queue', 'routing_key') if
                k in self._fields['delivery']}


class DeliveryAction(Action):
    __metaclass__ = ABCMeta

    def __init__(self, message):
        super(DeliveryAction, self).__init__(message)

    @staticmethod
    def __get_accept_graph(message_id):
        form = CGraph()
        for t in _accepted_template:
            form.add(t)
        form.add((_accept_node, CURATOR.messageId, Literal(message_id, datatype=TYPES.UUID)))
        form.add((_accept_node, CURATOR.submittedOn, Literal(datetime.now())))
        for (prefix, ns) in _accepted_template.namespaces():
            form.bind(prefix, ns)
        return form

    def __reply_accepted(self):
        graph = self.__get_accept_graph(self.request.message_id)
        self.sink.state = 'accepted'
        reply(graph.serialize(format='turtle'), **self.request.channel)

    def submit(self):
        super(DeliveryAction, self).submit()
        self.__reply_accepted()


class DeliverySink(Sink):
    __metaclass__ = ABCMeta

    @abstractmethod
    def _save(self, action):
        super(DeliverySink, self)._save(action)
        self._pipe.sadd('deliveries', self._request_id)
        broker_b64 = base64.b64encode('|'.join(action.request.broker.values()))
        channel_b64 = base64.b64encode('|'.join(action.request.channel.values()))
        self._pipe.hmset('channels:{}'.format(channel_b64), action.request.channel)
        self._pipe.hmset('brokers:{}'.format(broker_b64), action.request.broker)
        self._pipe.set('{}:channel'.format(self._request_key), channel_b64)
        self._pipe.set('{}:broker'.format(self._request_key), broker_b64)

    @abstractmethod
    def _load(self):
        super(DeliverySink, self)._load()
        try:
            del self._dict_fields['state']
        except KeyError:
            pass

    @property
    def state(self):
        return r.hget('deliveries:{}'.format(self._request_id), 'state')

    @state.setter
    def state(self, value):
        with r.pipeline(transaction=True) as p:
            p.multi()
            p.hset('deliveries:{}'.format(self._request_id), 'state', value)
            p.execute()


class DeliveryResponse(Response):
    __metaclass__ = ABCMeta

    def __init__(self, rid):
        super(DeliveryResponse, self).__init__(rid)

    @abstractmethod
    def build(self):
        super(DeliveryResponse, self).build()
