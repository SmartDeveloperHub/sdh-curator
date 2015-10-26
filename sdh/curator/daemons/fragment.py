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
from abc import ABCMeta, abstractmethod

from threading import Thread
import logging
import time

from agora.client.agora import Agora, AGORA, RDF
from sdh.curator.server import app
from sdh.curator.store import r
from sdh.curator.store.triples import graph as triples
from sdh.curator.daemons.delivery import build_response

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.daemons.fragment')
agora_host = app.config['AGORA']
agora_client = Agora(agora_host)


class FragmentPlugin(object):
    __plugins = []

    @abstractmethod
    def consume(self, sink, quad, graph):
        pass

    @abstractmethod
    def complete(self, sink):
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
    map(lambda (prefix, uri): triples.bind(prefix, uri), source_graph.namespaces())


def __consume_quad(quad, graph, sinks):
    def __sink_consume():
        for rid in sinks:
            sink = sinks[rid]
            try:
                plugin.consume(sink, quad, graph)
            except Exception as e:
                sink.remove()
                yield rid
                log.warning(e.message)

    for plugin in FragmentPlugin.plugins():
        invalid_sinks = list(__sink_consume())
        for _ in invalid_sinks:
            del sinks[_]
        triples.add(quad[1:])  # Don't add context


def __notify_completion(sinks):
    for plugin in FragmentPlugin.plugins():
        try:
            for sink in sinks.values():
                sink.state = 'ready'
                plugin.complete(sink)
        except Exception as e:
            log.warning(e.message)


def __pull_fragment(fid):
    def __load_fragment_requests():
        requests_ = r.smembers('fragments:{}:requests'.format(fid))
        sinks_ = {rid: build_response(rid).sink for rid in requests_}
        return requests_, sinks_

    tps = r.smembers('fragments:{}:gp'.format(fid))
    requests, r_sinks = __load_fragment_requests()
    log.debug('Pulling fragment {}, described by {}...'.format(fid, tps))
    fgm_gen, _, graph = agora_client.get_fragment_generator('{ %s }' % ' . '.join(tps),
                                                            on_load=__on_load_seed)
    __bind_prefixes(graph)

    for quad in fgm_gen:
        __consume_quad(quad, graph, r_sinks)
        if r.scard('fragments:{}:requests'.format(fid)) != len(requests):
            requests, r_sinks = __load_fragment_requests()
    with r.pipeline(transaction=True) as p:
        p.multi()
        state_key = 'fragments:{}:sync'.format(fid)
        p.set(state_key, True)
        p.expire(state_key, 10)
        p.execute()
    __notify_completion(r_sinks)


def __collect_fragments():
    log.info('Collector thread started')
    while True:
        map(lambda fid: __pull_fragment(fid), filter(lambda x: r.get('fragments:{}:sync'.format(x)) is None,
                                                     r.smembers('fragments')))
        time.sleep(1)


th = Thread(target=__collect_fragments)
th.daemon = True
th.start()
