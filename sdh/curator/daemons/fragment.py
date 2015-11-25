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
from datetime import datetime as dt, datetime
from concurrent.futures.thread import ThreadPoolExecutor
from agora.client.agora import Agora, AGORA
from sdh.curator.server import app
from sdh.curator.store import r
from sdh.curator.store.triples import cache as cache, add_stream_triple, load_stream_triples, graph_provider
from sdh.curator.daemons.delivery import build_response
from redis.lock import Lock
from rdflib import RDF, RDFS

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.daemons.fragment')
agora_host = app.config['AGORA']
agora_client = Agora(agora_host)

thp = ThreadPoolExecutor(max_workers=4)

fragment_locks = r.keys('fragments:*:lock')
for flk in fragment_locks:
    r.delete(flk)

fragment_pullings = r.keys('fragments:*:pulling')
for fpk in fragment_pullings:
    r.delete(fpk)

fragment_consumers = r.keys('fragments:*:consumers')
for fck in fragment_consumers:
    r.delete(fck)


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

    def sink_aware(self):
        return True

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
    map(lambda (prefix, uri): cache.bind(prefix, uri), source_graph.namespaces())


def map_variables(tp, mapping):
    if mapping is None:
        return tp
    return tuple(map(lambda x: mapping.get(x, x), tp))


def __consume_quad(fid, (c, s, p, o), graph, sinks=None):
    def __sink_consume():
        for rid in filter(lambda _: isinstance(sinks[_], plugin.sink_class), sinks):
            sink = sinks[rid]
            try:
                plugin.consume(fid, (map_variables(c, sink.mapping), s, p, o), graph, sink)
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
            filtered_sinks = filter(lambda _: isinstance(sinks[_], plugin.sink_class), sinks)
            for rid in filtered_sinks:
                sink = sinks[rid]
                if sink.delivery == 'accepted':
                    sink.delivery = 'ready'
                if plugin.sink_aware:
                    plugin.complete(fid, sink)
            if not plugin.sink_aware:
                plugin.complete(fid)
        except Exception as e:
            log.warning(e.message)


def __triple_pattern(graph, c):
    def extract_node_id(node):
        nid = node
        if (node, RDF.type, AGORA.Variable) in graph:
            nid = list(graph.objects(node, RDFS.label)).pop()
        elif (node, RDF.type, AGORA.Literal) in graph:
            nid = list(graph.objects(node, AGORA.value)).pop()
        return nid

    predicate = list(graph.objects(c, AGORA.predicate)).pop()
    subject_node = list(graph.objects(c, AGORA.subject)).pop()
    object_node = list(graph.objects(c, AGORA.object)).pop()
    subject = extract_node_id(subject_node)
    obj = extract_node_id(object_node)

    return str(subject), predicate.n3(graph.namespace_manager), str(obj)


def __replace_fragment(fid):
    tps = cache.get_context(fid).subjects(RDF.type, AGORA.TriplePattern)
    cache.remove_context(cache.get_context('/' + fid))
    for tp in tps:
        cache.remove_context(cache.get_context(str((fid, __triple_pattern(cache, tp)))))
    fragment_triples = load_stream_triples(fid, calendar.timegm(dt.now().timetuple()))
    for c, s, p, o in fragment_triples:
        cache.get_context(str((fid, c))).add((s, p, o))
        cache.get_context('/' + fid).add((s, p, o))
    with r.pipeline() as pipe:
        pipe.delete('fragments:{}:stream'.format(fid))
        pipe.execute()


def wait_consumers(fid):
    def get_consumers():
        try:
            return int(r.get('fragments:{}:consumers'.format(fid)))
        except TypeError:  # The key does not exist yet
            return 0

    consumers = get_consumers()
    log.debug('Current fragment {} consumers: {}'.format(fid, consumers))
    start = datetime.now()
    while consumers:
        log.debug('{} active consumers!'.format(consumers))
        time.sleep(0.1)
        consumers = get_consumers()
        # if (datetime.now() - start).seconds > 10:
        #     break


def __cache_plan_context(fid, graph):
    try:
        cache.remove_context(cache.get_context(fid))
        fid_context = cache.get_context(fid)
        tps = graph.subjects(RDF.type, AGORA.TriplePattern)
        for tp in tps:
            for (s, p, o) in graph.triples((tp, None, None)):
                fid_context.add((s, p, o))
                for t in graph.triples((o, None, None)):
                    fid_context.add(t)
    except Exception, e:
        log.error(e.message)


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
    log.info('Pulling fragment {}, described by {}...'.format(fid, tps))
    fgm_gen, _, graph = agora_client.get_fragment_generator('{ %s }' % ' . '.join(tps), workers=8,
                                                            provider=graph_provider)

    triple_patterns = {tpn: __triple_pattern(graph, tpn) for tpn in
                       graph.subjects(RDF.type, AGORA.TriplePattern)}
    fragment_contexts = {tpn: (fid, triple_patterns[tpn]) for tpn in triple_patterns}
    __bind_prefixes(graph)

    lock_key = 'fragments:{}:lock'.format(fid)
    lock = r.lock(lock_key, lock_class=Lock)
    lock.acquire()
    with r.pipeline(transaction=True) as p:
        p.multi()
        p.set('fragments:{}:pulling'.format(fid), True)
        p.delete('fragments:{}:contexts'.format(fid))
        for tpn in fragment_contexts.keys():
            p.sadd('fragments:{}:contexts'.format(fid), fragment_contexts[tpn])
        p.execute()
    lock.release()

    for (c, s, p, o) in fgm_gen:
        lock.acquire()
        if add_stream_triple(fid, triple_patterns[c], (s, p, o)):
            __consume_quad(fid, (triple_patterns[c], s, p, o), graph, sinks=r_sinks)
        lock.release()
        if r.scard('fragments:{}:requests'.format(fid)) != len(requests):
            requests, r_sinks = __load_fragment_requests()

    wait_consumers(fid)
    lock.acquire()
    __replace_fragment(fid)
    __cache_plan_context(fid, graph)
    with r.pipeline(transaction=True) as p:
        p.multi()
        sync_key = 'fragments:{}:sync'.format(fid)
        p.set(sync_key, True)
        p.set('fragments:{}:updated'.format(fid), dt.now())
        p.delete('fragments:{}:pulling'.format(fid))
        p.expire(sync_key, 60)
        p.execute()
    __notify_completion(fid, r_sinks)
    lock.release()


def __collect_fragments():
    log.info('Collector thread started')

    futures = {}
    while True:
        for fid in filter(
                lambda x: r.get('fragments:{}:sync'.format(x)) is None and r.get(
                    'fragments:{}:pulling'.format(x)) is None,
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
