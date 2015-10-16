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

from sdh.curator.actions import Request
import logging

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.delivery')


class DeliveryRequest(Request):
    def __init__(self):
        super(DeliveryRequest, self).__init__()
        self._target_resource = self._message_id = self._submitted_on = self._submitted_by = None
        self._exchange_name = self._queue_name = self._routing_key = None
        self._host = self._port = self._virtual_host = None

    def _extract_content(self):
        q_res = self._graph.query("""SELECT ?t ?m ?d ?a ?ex ?q ?rk ?h ?p ?v WHERE {
                                  [] a curator:EnrichmentRequest;
                                     curator:targetResource ?t;
                                     curator:messageId ?m;
                                     curator:submittedOn ?d;
                                     curator:submittedBy [
                                        curator:agentId ?a
                                     ];
                                     curator:replyTo [
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
            raise SyntaxError('Invalid enrichment request')

        request_fields = q_res.pop()

        if not all(request_fields):
            raise ValueError('Missing enrichment request fields')

        (self._target_resource,
         self._message_id,
         self._submitted_on,
         self._submitted_by,
         self._exchange_name,
         self._queue_name,
         self._routing_key,
         self._host, self._port,
         self._virtual_host) = request_fields
