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
import calendar
from threading import Thread
import logging
import time

from abc import abstractmethod, abstractproperty
from datetime import datetime as dt
from concurrent.futures.thread import ThreadPoolExecutor
from agora.client.agora import Agora, AGORA
from sdh.curator.server import app
from sdh.curator.store import r
from sdh.curator.store.triples import cache as cache, front, add_stream_triple, load_stream_triples
from sdh.curator.daemons.delivery import build_response
from redis.lock import Lock

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.daemons.fragment')
agora_host = app.config['AGORA']
agora_client = Agora(agora_host)

thp = ThreadPoolExecutor(max_workers=4)


class FragmentPlugin(object):
    __plugins = []

    @abstractmethod
    def consume(self, fid, quad, graph, *args):
        pass

    @abstractmethod
    def complete(self, fid, *args):
        pass

    @abstractproperty
    def sink_class(self):
        pass

    @classmethod
    def register(cls, p):
        if issubclass(p, cls):
            cls.__plugins.append(p())
        else:
            raise ValueError('{} is not a valid fragment plugin'.format(p))

    @classmethod
    def plugins(cls):
        return cls.__plugins[:]


def __on_load_seed(s, _):
    log.debug('{} dereferenced'.format(s))


def __bind_prefixes(source_graph):
    map(lambda (prefix, uri): (cache.bind(prefix, uri), front.bind(prefix, uri)), source_graph.namespaces())


def __consume_quad(fid, (c, s, p, o), graph, sinks=None):
    def __sink_consume():
        for rid in filter(lambda _: isinstance(sinks[_], plugin.sink_class), sinks):
            sink = sinks[rid]
            try:
                plugin.consume(fid, (c, s, p, o), graph, sink)
            except Exception as e:
                sink.remove()
                yield rid
                log.warning(e.message)

    def __generic_consume():
        try:
            plugin.consume(fid, (c, s, p, o), graph)
        except Exception as e:
            log.warning(e.message)

    for plugin in FragmentPlugin.plugins():
        if plugin.sink_class is not None:
            invalid_sinks = list(__sink_consume())
            for _ in invalid_sinks:
                del sinks[_]
        else:
            __generic_consume()


def __notify_completion(fid, sinks):
    for plugin in FragmentPlugin.plugins():
        try:
            if plugin.sink_class is not None:
                for rid in filter(lambda _: isinstance(sinks[_], plugin.sink_class), sinks):
                    sink = sinks[rid]
                    if sink.delivery == 'accepted':
                        sink.delivery = 'ready'
                    plugin.complete(fid, sink)
            else:
                plugin.complete(fid)
        except Exception as e:
            log.warning(e.message)


def __pull_fragment(fid):
    def __load_fragment_requests():
        requests_ = r.smembers('fragments:{}:requests'.format(fid))
        sinks_ = {}
        for rid in requests_:
            try:
                sinks_[rid] = build_response(rid).sink
            except Exception, e:
                log.warning(e.message)
                with r.pipeline(transaction=True) as p:
                    p.multi()
                    p.srem('fragments:{}:requests'.format(fid), rid)
                    p.execute()
        return requests_, sinks_

    tps = r.smembers('fragments:{}:gp'.format(fid))
    requests, r_sinks = __load_fragment_requests()
    log.debug('Pulling fragment {}, described by {}...'.format(fid, tps))
    fgm_gen, _, graph = agora_client.get_fragment_generator('{ %s }' % ' . '.join(tps),
                                                            on_load=__on_load_seed)
    __bind_prefixes(graph)

    lock_key = 'fragments:{}:lock'.format(fid)
    lock = r.lock(lock_key, lock_class=Lock)
    lock.acquire()
    cache.remove_context(cache.get_context(fid))
    with r.pipeline() as pipe:
        pipe.delete('fragments:{}:stream'.format(fid))
        pipe.execute()
    fragment_triples = load_stream_triples(fid, calendar.timegm(dt.now().timetuple()))
    for ft in fragment_triples:
        cache.get_context(fid).add(ft)
    with r.pipeline(transaction=True) as p:
        p.multi()
        p.set('fragments:{}:pulling'.format(fid), True)
        p.execute()
    lock.release()

    for (c, s, p, o) in fgm_gen:
        if (s, p, o) not in cache:
            cache.get_context(fid).add((s, p, o))
            lock.acquire()
            add_stream_triple(fid, (s, p, o))
            __consume_quad(fid, (c, s, p, o), graph, sinks=r_sinks)
            lock.release()
        if r.scard('fragments:{}:requests'.format(fid)) != len(requests):
            requests, r_sinks = __load_fragment_requests()

    lock.acquire()
    with r.pipeline(transaction=True) as p:
        p.multi()
        sync_key = 'fragments:{}:sync'.format(fid)
        p.set(sync_key, True)
        p.set('fragments:{}:updated'.format(fid), dt.now())
        p.delete('fragments:{}:pulling'.format(fid))
        p.expire(sync_key, 10)
        p.execute()
    __notify_completion(fid, r_sinks)
    lock.release()


def __collect_fragments():
    log.info('Collector thread started')

    futures = {}
    while True:
        for fid in filter(lambda x: r.get('fragments:{}:sync'.format(x)) is None,
                          r.smembers('fragments')):
            if fid in futures:
                if futures[fid].done():
                    del futures[fid]
            if fid not in futures:
                futures[fid] = thp.submit(__pull_fragment, fid)
        time.sleep(1)


th = Thread(target=__collect_fragments)
th.daemon = True
th.start()
