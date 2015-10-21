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
from rdflib import URIRef, RDFS

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.collect')
agora_host = app.config['AGORA']
agora_client = Agora(agora_host)


def __collect_fragments():
    log.debug('Started collector thread!')
    while True:
        for rid in r.smembers('fragments'):
            tps = r.smembers('requests:{}:gp'.format(rid))
            target = URIRef(r.get('requests:{}:enrich'.format(rid)))
            links = dict(map(lambda (pr, v): (v, URIRef(pr)),
                             [eval(pair) for pair in r.smembers('requests:{}:enrich:links'.format(rid))]))

            log.debug('Request {} asks for {}'.format(rid, list(tps)))
            log.debug('[TEST] Synchronously retrieving the corresponding fragment...')
            fgm_gen, _, graph = agora_client.get_fragment_generator('{ %s }' % ' . '.join(tps))
            for c, s, p, o in fgm_gen:
                var_candidate = list(graph.objects(c, AGORA.subject))[0]
                if (var_candidate, RDF.type, AGORA.Variable) in graph:
                    var_label = list(graph.objects(var_candidate, RDFS.label))[0]
                    if var_label in links:
                        print u'{} {} {} .'.format(target, links[var_label], s)
                        # print u'{} {} {} .'.format(s.n3(graph.namespace_manager),
                        #                            p.n3(graph.namespace_manager),
                        #                            o.n3(graph.namespace_manager))
            print 'done'
        time.sleep(1)


th = Thread(target=__collect_fragments)
th.daemon = True
th.start()