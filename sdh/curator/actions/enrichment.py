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

from sdh.curator.actions import Action
import logging
from rdflib import RDF
from rdflib.namespace import Namespace

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.enrichment')
CURATOR = Namespace('http://www.smartdeveloperhub.org/vocabulary/curator#')
AMQP = Namespace('http://www.smartdeveloperhub.org/vocabulary/amqp#')

class EnrichmentAction(Action):
    def run(self):
        log.debug('Received a potential enrichment request')
        try:
            request_node = self._request_graph.subjects(RDF.type, CURATOR.EnrichmentRequest, card=1)
        except ValueError:
            raise SyntaxError('There is not an only enrichment request specified in the message')

        try:
            target_resource = self._request_graph.objects(request_node, CURATOR.targetResource, card=1)
        except ValueError as e:
            raise SyntaxError('Invalid number of target resources: {}'.format(e.message))
        log.debug('Found a request to enrich {}!'.format(target_resource))

        try:
            message_id = self._request_graph.objects(request_node, CURATOR.messageId, card=1)
        except ValueError as e:
            raise SyntaxError('Invalid number of message identifiers: {}'.format(e.message))
        log.debug('Message identified as {}'.format(message_id))

        try:
            submitted_on = self._request_graph.objects(request_node, CURATOR.submittedOn, card=1)
        except ValueError as e:
            raise SyntaxError('Invalid number of submission dates: {}'.format(e.message))
        log.debug('Message submitted on {}'.format(submitted_on))

        try:
            submitter_node = self._request_graph.objects(request_node, CURATOR.submittedBy, card=1)
            submitted_by = self._request_graph.objects(submitter_node, CURATOR.agentId, card=1)
        except ValueError as e:
            raise SyntaxError('Invalid number of submitters: {}'.format(e.message))
        log.debug('Message submitted by {}'.format(submitted_by))

        dh_res = self._request_graph.query("""SELECT ?ex ?q ?rk ?h ?p ?v WHERE {
                                              [] a curator:EnrichmentRequest;
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
        dh_res = list(dh_res)
        if len(dh_res) != 1:
            raise SyntaxError('Invalid delivery channel specification')

        (exchange_name, queue_name, routing_key, host, port, virtual_host) = dh_res.pop()
        log.debug('Delivery change to reply to: {}')
