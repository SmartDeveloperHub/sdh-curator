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

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.daemons.delivery')


def __build_response(response_class, rid):
    (module_name, class_name) = tuple(response_class.split('.'))
    module = __import__(module_name)
    class_ = getattr(module, class_name)
    instance = class_(rid)
    return instance.build()


def __deliver_responses():
    import time
    from sdh.curator.store import r
    from sdh.curator.messaging.reply import reply
    log.info('Delivery thread started')
    while True:
        for rid in r.smembers('deliveries'):
            delivery_data = r.hgetall('deliveries:{}'.format(rid))
            if delivery_data['state'] == 'ready':
                log.debug('Delivery-{} in process...'.format(rid))
                channel_b64 = r.get('requests:{}:channel'.format(rid))
                channel_dict = r.hgetall('channels:{}'.format(channel_b64))
                response_class = r.hget('requests:{}'.format(rid), 'response_class')
                response = __build_response(response_class, rid)
                reply(response, **channel_dict)
                with r.pipeline(transaction=True) as p:
                    p.multi()
                    p.hset('deliveries:{}'.format(rid), 'state', 'sent')
                    p.execute()

        time.sleep(1)


__thread = Thread(target=__deliver_responses)
__thread.daemon = True
__thread.start()
