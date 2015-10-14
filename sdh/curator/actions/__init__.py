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
import StringIO

import pkgutil
from sdh.curator.actions.utils import search_module, CGraph
import logging

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions')


class Action(object):
    def __init__(self, message):
        self._request_graph = CGraph()
        self._request_graph.bind('curator', 'http://www.smartdeveloperhub.org/vocabulary/curator#')
        self._request_graph.bind('amqp', 'http://www.smartdeveloperhub.org/vocabulary/amqp#')
        self._request_graph.parse(StringIO.StringIO(message), format='turtle')
        log.debug('Received message:\n{}'.format(self._request_graph.serialize(format='turtle')))

    def run(self):
        print self._request_graph.serialize(format='turtle')


def execute(*args, **kwargs):
    cand_modules = filter(lambda x: not x[2] and x[1] == args[0], pkgutil.iter_modules(path=['sdh/curator/actions']))
    if len(cand_modules) > 1:
        raise EnvironmentError('Too many modules for action {}'.format(args[0]))
    elif len(cand_modules) == 0:
        raise AttributeError('Unknown action: "{}"'.format(args[0]))

    (importer, name, _) = cand_modules.pop()
    loader = importer.find_module('sdh.curator.actions.{}'.format(name))
    try:
        _, clz = search_module(loader.get_filename(),
                               lambda (_, cl): issubclass(cl, Action) and cl != Action).pop()
        clz(kwargs.get('data', None)).run()
    except IndexError:
        raise EnvironmentError('Action module found but class is missing: "{}"'.format(name))
