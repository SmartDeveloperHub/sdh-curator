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
    response_class = r.hget('requests:{}'.format(rid), 'response_class')
    (module_name, class_name) = tuple(response_class.split('.'))
    module = __import__(module_name)
    class_ = getattr(module, class_name)
    instance = class_(rid)
    return instance


def __deliver_responses():
    import time

    from sdh.curator.messaging.reply import reply
    log.info('Delivery thread started')
    while True:
        for rid in r.smembers('deliveries:ready'):
            response = build_response(rid)
            if response.sink.state == 'ready':
                log.debug('Delivery-{} in process...'.format(rid))
                reply(response.build(), **response.sink.channel)
                response.sink.state = 'sent'

        time.sleep(1)


__thread = Thread(target=__deliver_responses)
__thread.daemon = True
__thread.start()
