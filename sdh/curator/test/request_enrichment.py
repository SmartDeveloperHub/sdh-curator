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
import StringIO

__author__ = 'Fernando Serena'

import pika
import sys
from rdflib import Graph, URIRef, RDF
from rdflib.namespace import Namespace
import os

CURATOR = Namespace('http://www.smartdeveloperhub.org/vocabulary/curator#')


def callback(ch, method, properties, body):
    g = Graph()
    g.parse(StringIO.StringIO(body), format='turtle')
    if len(list(g.subjects(RDF.type, CURATOR.Accepted))) == 1:
        print 'Request accepted!'
    else:
        print g.serialize(format='turtle')


connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='localhost'))
channel = connection.channel()

routing_key = 'request.enrichment'

graph = Graph()
script_dir = os.path.dirname(__file__)
with open(os.path.join(script_dir, 'request_example.ttl')) as f:
    graph.parse(file=f, format='turtle')

message = graph.serialize(format='turtle')

channel.basic_publish(exchange='curator',
                      routing_key=routing_key,
                      body=message)

channel.queue_declare(queue='builds' or '', durable=True)
channel.queue_bind(exchange='curator', queue='builds', routing_key='response.#')
channel.basic_consume(callback, queue='builds', no_ack=True)

channel.start_consuming()
