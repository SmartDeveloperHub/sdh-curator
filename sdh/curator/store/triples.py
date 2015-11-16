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

from rdflib import ConjunctiveGraph, URIRef, Literal
from sdh.curator.server import app
import logging
import calendar
from datetime import datetime as dt
from sdh.curator.store import r
import os

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.store.triples')


def load_stream_triples(fid, until):
    def __triplify(x):
        def __term(elm):
            if elm.startswith('<'):
                return URIRef(elm.lstrip('<').rstrip('>'))
            else:
                (value, ty) = tuple(elm.split('^^'))
                return Literal(value.replace('"', ''), datatype=URIRef(ty.lstrip('<').rstrip('>')))

        c, s, p, o = eval(x)
        return c, __term(s), __term(p), __term(o)

    for x in r.zrangebyscore('fragments:{}:stream'.format(fid), '-inf', '{}'.format(float(until))):
        yield __triplify(x)


def add_stream_triple(fid, tp, (s, p, o), timestamp=None):
    if timestamp is None:
        timestamp = calendar.timegm(dt.utcnow().timetuple())
    quad = (tp, s.n3(), p.n3(), o.n3())
    stream_key = 'fragments:{}:stream'.format(fid)
    not_found = not bool(r.zscore(stream_key, quad))
    if not_found:
        with r.pipeline() as pipe:
            pipe.zadd(stream_key, timestamp, quad)
            pipe.execute()
    return not_found


store_mode = app.config['STORE']
if 'persist' in store_mode:
    if not os.path.exists('store'):
        os.makedirs('store')
    log.info('Loading known triples...')
    cache = ConjunctiveGraph('Sleepycat')
    front = ConjunctiveGraph('Sleepycat')
    cache.open('store/cache', create=True)
    front.open('store/front', create=True)
else:
    cache = ConjunctiveGraph()
    front = ConjunctiveGraph()

cache.store.graph_aware = False
front.store.graph_aware = False
log.info('Ready')
