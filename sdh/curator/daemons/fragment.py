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

from threading import Thread
import logging
import time

from agora.client.agora import Agora, AGORA, RDF
from sdh.curator.server import app
from sdh.curator.store import r
from sdh.curator.store.triples import graph as triples, enrichment_graph as enrich_graph
from sdh.curator.daemons.delivery import build_response
from rdflib import URIRef, RDFS

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.daemons.fragment')
agora_host = app.config['AGORA']
agora_client = Agora(agora_host)


def __on_load_seed(s, _):
    print '{} dereferenced'.format(s)


def __collect_fragments():
    log.info('Collector thread started')
    while True:
        for rid in r.smembers('fragments'):
            r_sink = build_response(rid).sink
            if r_sink.state == 'accepted' and not r_sink.backed:
                tps = r_sink.gp
                target = r_sink.target_resource
                links = dict(map(lambda (link, v): (v, link), r_sink.target_links))

                log.debug('Request-{} asked for {}'.format(rid, list(tps)))
                log.debug('[TEST] Synchronously retrieving the corresponding fragment...')
                fgm_gen, _, graph = agora_client.get_fragment_generator('{ %s }' % ' . '.join(tps),
                                                                        on_load=__on_load_seed)
                for (prefix, uri) in graph.namespaces():
                    triples.bind(prefix, uri)
                for c, s, p, o in fgm_gen:
                    var_candidate = list(graph.objects(c, AGORA.subject))[0]
                    if (var_candidate, RDF.type, AGORA.Variable) in graph:
                        var_label = str(list(graph.objects(var_candidate, RDFS.label))[0])
                        if var_label in links:
                            enrich_graph.add((target, links[var_label], s))
                            print u'{} {} {} .'.format(target.n3(), links[var_label].n3(graph.namespace_manager),
                                                       s.n3())
                    triples.add((s, p, o))
                r_sink.state = 'ready'
                r_sink.backed = True
        time.sleep(1)


th = Thread(target=__collect_fragments)
th.daemon = True
th.start()
