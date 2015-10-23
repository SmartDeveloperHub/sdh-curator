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
from pika.exceptions import ChannelClosed

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.messaging.reply')


def reply(message, exchange=None, routing_key=None, queue=None, mandatory=False):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    try:
        channel = connection.channel()
        if not any([exchange, routing_key, queue]):
            raise AttributeError('Insufficient delivery channel parameters')

        exchange = '' if exchange is None else exchange
        routing_key = '' if routing_key is None else routing_key
        if not exchange and not routing_key:
            routing_key = queue
            mandatory = True
            channel.confirm_delivery()

        sent = channel.basic_publish(exchange=exchange,
                                     routing_key=routing_key,
                                     body=message,
                                     mandatory=mandatory)

        if mandatory and not sent:
            raise EnvironmentError('The queue {} does not exist'.format(queue))
        log.debug('Sent message to delivery channel: \n -exchange: {}\n -routing_key: {}'.format(
            exchange, routing_key
        ))
    except ChannelClosed:
        raise EnvironmentError('The queue {} does not exist'.format(queue))
    finally:
        connection.close()
