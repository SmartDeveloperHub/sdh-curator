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
import calendar
import logging

from abc import abstractproperty, abstractmethod, ABCMeta
from sdh.curator.actions.core.utils import CGraph
from sdh.curator.store import r

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.base')
R_INDEX_KEY = 'requests:index'


def _fullname(f):
    def wrapper():
        clz = f()
        return clz.__module__ + '.' + clz.__name__

    return wrapper


class Action(object):
    __metaclass__ = ABCMeta

    def __init__(self, message):
        self.__message = message
        self.__action_id = None
        self.__request_id = None
        self.__request_key = None

    @abstractproperty
    def request(self):
        pass

    @classmethod
    def response_class(cls):
        pass

    @abstractproperty
    def sink(self):
        pass

    @property
    def request_id(self):
        return self.__request_id

    @property
    def id(self):
        return self.__action_id

    @property
    def request_key(self):
        return self.__request_key

    @abstractmethod
    def submit(self):
        if not issubclass(self.response_class(), Response):
            raise SystemError(
                'The response class for this action is invalid: {}'.format(self.response_class()))
        self.request.parse(self.__message)
        self.__action_id = u'{}@{}'.format(self.request.message_id, self.request.submitted_by)
        self.__request_id = self.sink.save(self)


class Sink(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self._pipe = r.pipeline(transaction=True)
        self._request_id = None
        self._request_key = None

    def load(self, rid):
        if not r.keys('requests:{}'.format(rid)):
            raise ValueError('Cannot load request: Unknown request id {}'.format(rid))
        self._request_id = rid
        self._request_key = 'requests:{}'.format(self._request_id)
        self._load()

    @abstractmethod
    def _load(self):
        self._dict_fields = r.hgetall('requests:{}'.format(self._request_id))

    def __getattr__(self, item):
        if item in self._dict_fields:
            return self._dict_fields[item]
        return super(Sink, self).__getattribute__(item)

    def save(self, action):
        index_pipe = r.pipeline(transaction=True)
        index_pipe.watch(R_INDEX_KEY)
        index_pipe.multi()
        self._request_id = r.get(R_INDEX_KEY) or 0
        index_pipe.incr(R_INDEX_KEY)
        index_pipe.execute()
        self._pipe.multi()
        self._save(action)
        self._pipe.execute()
        return self._request_id

    @abstractmethod
    def _save(self, action):
        if r.zscore('requests', action.id):
            raise ValueError('Duplicated request: {}'.format(action.id))
        submitted_by_ts = calendar.timegm(action.request.submitted_on.timetuple())
        self._pipe.zadd('requests', submitted_by_ts, action.id)
        self._pipe.incr('requests:index')
        self._request_key = 'requests:{}'.format(self._request_id)
        self._pipe.hmset(self._request_key, {'submitted_by': action.request.submitted_by,
                                             'submitted_on': action.request.submitted_on,
                                             'message_id': action.request.message_id,
                                             'response_class': _fullname(action.response_class)()})


class Request(object):
    def __init__(self):
        self._graph = CGraph()
        self._graph.bind('curator', 'http://www.smartdeveloperhub.org/vocabulary/curator#')
        self._graph.bind('amqp', 'http://www.smartdeveloperhub.org/vocabulary/amqp#')
        self._request_node = None
        self._fields = {}

    def parse(self, message):
        log.debug('Parsing message...')
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

        (self._request_node, self._fields['message_id'],
         self._fields['submitted_on'],
         self._fields['submitted_by']) = request_fields
        log.debug(
            """Parsed attributes of generic action request:
                -message id: {}
                -submitted on: {}
                -submitted by: {}""".format(
                self._fields['message_id'], self._fields['submitted_on'], self._fields['submitted_by']))

    @property
    def message_id(self):
        return self._fields['message_id'].toPython()

    @property
    def submitted_by(self):
        return self._fields['submitted_by'].toPython()

    @property
    def submitted_on(self):
        return self._fields['submitted_on'].toPython()


class Response(object):
    __metaclass__ = ABCMeta

    def __init__(self, rid):
        self._request_id = rid
        self._request_key = 'requests:{}'.format(rid)

    @abstractmethod
    def build(self):
        pass

    @abstractproperty
    def sink(self):
        pass
