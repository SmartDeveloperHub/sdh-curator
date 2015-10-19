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

from abc import ABCMeta, abstractproperty, abstractmethod
from sdh.curator.actions.core.base import Request, Action

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.delivery')


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

        if not all(request_fields):
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

    @abstractproperty
    def request(self):
        pass

    def __init__(self, message):
        super(DeliveryAction, self).__init__(message)

    def submit(self):
        super(DeliveryAction, self).submit()

    @abstractmethod
    def _persist(self):
        super(DeliveryAction, self)._persist()
        self._pipe.sadd('deliveries', self._request_id)
        self._pipe.hmset('{}:channel'.format(self._request_key), self.request.channel)
        self._pipe.hmset('{}:broker'.format(self._request_key), self.request.broker)
