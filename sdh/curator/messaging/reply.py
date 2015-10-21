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

import pika

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.messaging.reply')


def reply(message, exchange=None, routing_key=None, queue=None):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    if not any([exchange, routing_key, queue]):
        raise AttributeError('Insufficient delivery channel parameters')

    channel.exchange_declare(exchange='curator',
                             type='topic', durable=True)

    exchange = '' if exchange is None else exchange
    routing_key = '' if routing_key is None else routing_key

    channel.basic_publish(exchange=exchange,
                          routing_key=routing_key,
                          body=message)
    log.debug('Sent message to delivery channel: \n -exchange: {}\n -routing_key: {}'.format(
        exchange, queue, routing_key
    ))
    connection.close()
