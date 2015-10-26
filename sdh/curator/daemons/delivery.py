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
from threading import Thread
from sdh.curator.store import r

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.daemons.delivery')


def build_response(rid):
    from sdh.curator.actions import get_instance
    response_class = r.hget('requests:{}'.format(rid), 'response_class')
    if response_class is None:
        raise AttributeError('Cannot create a response for {}'.format(rid))
    (module_name, class_name) = tuple(response_class.split('.'))
    return get_instance(module_name, class_name, rid)


def __deliver_responses():
    import time

    from sdh.curator.messaging.reply import reply
    log.info('Delivery thread started')
    while True:
        for rid in r.smembers('deliveries:ready'):
            try:
                response = build_response(rid)
                if response.sink.state == 'ready':
                    log.debug('Delivery-{} in process...'.format(rid))
                    message = response.build()
                    reply(message, **response.sink.channel)
                    log.debug('Response sent for request number {}'.format(rid))
                    response.sink.state = 'sent'
            except AttributeError, e:
                log.error(e.message)
                # with r.pipeline(transaction=True) as p:
                #     p.srem('deliveries:ready', rid)
                #     p.execute()

                # A response couldn't be created

        time.sleep(1)


__thread = Thread(target=__deliver_responses)
__thread.daemon = True
__thread.start()
