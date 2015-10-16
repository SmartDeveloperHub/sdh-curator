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
import logging

from abc import abstractproperty, abstractmethod, ABCMeta
from sdh.curator.actions.core.utils import search_module, CGraph
from sdh.curator.actions.core.store import r

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions')


class Action(object):
    __metaclass__ = ABCMeta

    def __init__(self, message):
        self.__message = message
        self._request = None

    @abstractproperty
    def request(self):
        pass

    def perform(self):
        self.request.parse(self.__message)


class Request(object):
    def __init__(self):
        self._graph = CGraph()
        self._graph.bind('curator', 'http://www.smartdeveloperhub.org/vocabulary/curator#')
        self._graph.bind('amqp', 'http://www.smartdeveloperhub.org/vocabulary/amqp#')
        self._request_node = self._message_id = self._submitted_on = self._submitted_by = None

    def parse(self, message):
        self._graph.parse(StringIO.StringIO(message), format='turtle')
        self._extract_content()

    @abstractmethod
    def _extract_content(self):
        q_res = self._graph.query("""SELECT ?node ?m ?d ?a WHERE {
                                        ?node curator:messageId ?m;
                                              curator:submittedOn ?d;
                                              curator:submittedBy [
                                                 curator:agentId ?a
                                              ]
                                     }""")
        q_res = list(q_res)
        if len(q_res) != 1:
            raise SyntaxError('Invalid request')

        request_fields = q_res.pop()

        if not all(request_fields):
            raise ValueError('Missing fields for generic request')

        (self._request_node, self._message_id,
         self._submitted_on,
         self._submitted_by) = request_fields


def execute(*args, **kwargs):
    cand_modules = filter(lambda x: not x[2] and x[1] == args[0],
                          pkgutil.iter_modules(path=['sdh/curator/actions/ext']))
    if len(cand_modules) > 1:
        raise EnvironmentError('Too many modules for action {}'.format(args[0]))
    elif len(cand_modules) == 0:
        raise AttributeError('Unknown action: "{}"'.format(args[0]))

    (importer, name, _) = cand_modules.pop()
    loader = importer.find_module(name)
    try:
        _, clz = search_module(loader.get_filename(),
                               lambda (_, cl): issubclass(cl, Action) and cl != Action).pop()
        clz(kwargs.get('data', None)).perform()
    except IndexError:
        raise EnvironmentError('Action module found but class is missing: "{}"'.format(name))
