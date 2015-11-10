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
from concurrent.futures import wait, ALL_COMPLETED
from concurrent.futures.thread import ThreadPoolExecutor
from sdh.curator.messaging.reply import reply

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.daemons.delivery')
thp = ThreadPoolExecutor(max_workers=8)


def build_response(rid):
    from sdh.curator.actions import get_instance
    response_class = r.hget('requests:{}'.format(rid), 'response_class')
    if response_class is None:
        raise AttributeError('Cannot create a response for {}'.format(rid))
    (module_name, class_name) = tuple(response_class.split('.'))
    return get_instance(module_name, class_name, rid)


def __deliver_response(rid):
    response = None
    try:
        response = build_response(rid)
        if response.sink.delivery == 'ready':
            log.debug('Delivery-{} in process...'.format(rid))
            messages = response.build()
            message, headers = messages.next()
            reply(message, headers=headers, **response.sink.channel)
            for (message, headers) in messages:
                reply(message, headers=headers, **response.sink.channel)
            log.debug('Response sent for request number {}'.format(rid))
            if response.sink.delivery == 'ready':
                response.sink.delivery = 'sent'
    except StopIteration:
        log.debug('There is nothing to deliver for request number {}. Skipping...'.format(rid))
    except AttributeError, e:
        log.error(e.message)
        # A response couldn't be created
    except EnvironmentError, e:
        log.warning(e.message)
        if response is not None:
            response.sink.remove()
    except Exception, e:
        log.warning(e.message)


def __deliver_responses():
    import time

    log.info('Delivery thread started')
    futures = {}
    while True:
        ready = r.smembers('deliveries:ready')
        for rid in ready:
            if rid not in futures:
                futures[rid] = thp.submit(__deliver_response, rid)

        for obsolete_rid in set.difference(set(futures.keys()), ready):
            if obsolete_rid in futures and futures[obsolete_rid].done():
                del futures[obsolete_rid]

        time.sleep(1)


__thread = Thread(target=__deliver_responses)
__thread.daemon = True
__thread.start()
