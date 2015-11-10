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

import logging

from sdh.curator.actions.core.fragment import FragmentRequest, FragmentAction, FragmentResponse, FragmentSink
from sdh.curator.actions.core.utils import parse_bool
from sdh.curator.messaging.reply import reply
from sdh.curator.daemons.fragment import FragmentPlugin
from sdh.curator.store import r
from redis.lock import Lock
from datetime import datetime as dt

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.stream')


class StreamPlugin(FragmentPlugin):
    @property
    def sink_class(self):
        return StreamSink

    def consume(self, fid, (c, s, p, o), graph, *args):
        sink = args[0]
        if sink.delivery == 'sent':
            return
        if sink.stream:
            log.debug('[{}] Streaming fragment triple...'.format(sink.request_id))
            reply(u'ST {} {} {} .'.format(s.n3(), p.n3(), o.n3()),
                  **sink.channel)

    def complete(self, fid, *args):
        sink = args[0]
        if sink.delivery == 'streaming':
            sink.delivery = 'sent'
            reply('', headers={'state': 'end'}, **sink.channel)
        sink.stream = False


FragmentPlugin.register(StreamPlugin)


class StreamRequest(FragmentRequest):
    def __init__(self):
        super(StreamRequest, self).__init__()

    def _extract_content(self):
        super(StreamRequest, self)._extract_content()

        q_res = self._graph.query("""SELECT ?node WHERE {
                                        ?node a curator:StreamRequest .
                                    }""")

        q_res = list(q_res)
        if len(q_res) != 1:
            raise SyntaxError('Invalid query request')

        request_fields = q_res.pop()
        if not all(request_fields):
            raise ValueError('Missing fields for stream request')
        if request_fields[0] != self._request_node:
            raise SyntaxError('Request node does not match')


class StreamAction(FragmentAction):
    def __init__(self, message):
        self.__request = StreamRequest()
        self.__sink = StreamSink()
        super(StreamAction, self).__init__(message)

    @property
    def sink(self):
        return self.__sink

    @classmethod
    def response_class(cls):
        return StreamResponse

    @property
    def request(self):
        return self.__request

    def submit(self):
        super(StreamAction, self).submit()


class StreamSink(FragmentSink):
    def _remove(self, pipe):
        super(StreamSink, self)._remove(pipe)

    def __init__(self):
        super(StreamSink, self).__init__()

    def _save(self, action):
        super(StreamSink, self)._save(action)
        self.delivery = 'ready'

    def _load(self):
        super(StreamSink, self)._load()

    @property
    def stream(self):
        return parse_bool(r.hget('requests:{}'.format(self._request_id), 'stream'))

    @stream.setter
    def stream(self, value):
        with r.pipeline(transaction=True) as p:
            p.multi()
            p.hset('requests:{}'.format(self._request_id), 'stream', value)
            p.execute()


class StreamResponse(FragmentResponse):
    def __init__(self, rid):
        self.__sink = StreamSink()
        self.__sink.load(rid)
        super(StreamResponse, self).__init__(rid)

    @property
    def sink(self):
        return self.__sink

    def build(self):
        timestamp = calendar.timegm(dt.now().timetuple())
        lock = r.lock('fragment:{}:lock'.format(self.sink.fragment_id), lock_class=Lock)
        lock.acquire()
        fragment = None
        try:
            fragment, stream = self.fragment(stream=True, timestamp=timestamp)
            fragment = list(fragment)   # Ensure stream (redis sorted set) before it is updated
            if stream:
                self.sink.stream = True
                if fragment:
                    self.sink.delivery = 'mixing'
                else:
                    self.sink.delivery = 'streaming'
            else:
                if fragment:
                    self.sink.delivery = 'pushing'
                    log.debug('Fragment retrieved from cache for request number {}'.format(self._request_id))
                else:
                    self.sink.delivery = 'sent'
                    yield '', {'state': 'end'}
                self.sink.stream = False
        except Exception as e:
            log.warning(e.message)
            self.sink.stream = True
            self.sink.delivery = 'streaming'
        finally:
            lock.release()

        if fragment:
            log.debug('Building a stream result for request number {}'.format(self._request_id))
            for (s, p, o) in fragment:
                yield u'TS {} {} {} .'.format(s.n3(), p.n3(), o.n3()), {}   # (body, headers)

            lock.acquire()
            try:
                if self.sink.delivery == 'pushing' or (self.sink.delivery == 'mixing' and not self.sink.stream):
                    self.sink.delivery = 'sent'
                    yield '', {'state': 'end'}
                elif self.sink.delivery == 'mixing' and self.sink.stream:
                    self.sink.delivery = 'streaming'
            finally:
                lock.release()


