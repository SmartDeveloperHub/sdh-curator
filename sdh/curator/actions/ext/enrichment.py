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

import logging

from sdh.curator.actions.core.fragment import FragmentRequest, FragmentAction

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.enrichment')


class EnrichmentRequest(FragmentRequest):
    def __init__(self):
        super(EnrichmentRequest, self).__init__()
        self._target_resource = None
        self._target_links = set([])

    def _extract_content(self):
        super(EnrichmentRequest, self)._extract_content()

        q_res = self._graph.query("""SELECT ?node ?t WHERE {
                                        ?node a curator:EnrichmentRequest;
                                                curator:targetResource ?t
                                    }""")

        q_res = list(q_res)
        if len(q_res) != 1:
            raise SyntaxError('Invalid fragment request')

        request_fields = q_res.pop()
        if not all(request_fields):
            raise ValueError('Missing fields for fragment request')
        if request_fields[0] != self._request_node:
            raise SyntaxError('Request node does not match')

        (self._target_resource,) = request_fields[1:]

        log.debug("""Parsed attributes of an enrichment action request:
                      -target resource: {}""".format(self._target_resource))

        target_pattern = self._graph.predicate_objects(self._target_resource)
        for (pr, req_object) in target_pattern:
            self._target_links.add((pr, req_object))
            self.pattern.add((self._target_resource, pr, req_object))
        enrich_properties = set([pr for (pr, _) in self._target_links])
        log.debug(
            '<{}> is requested to be enriched with values for the following properties:\n{}'.format(
                self._target_resource,
                '\n'.join(enrich_properties)))
        log.debug('Extracted (enrichment) pattern graph:\n{}'.format(self.pattern.serialize(format='turtle')))

    @property
    def target_resource(self):
        return self._target_resource

    @property
    def target_links(self):
        return self._target_links.copy()


class EnrichmentAction(FragmentAction):
    def build_graph_pattern(self, v_labels=None):
        super(EnrichmentAction, self).build_graph_pattern(v_labels={self.request.target_resource: '?t'})

    @property
    def request(self):
        if self._request is None:
            self._request = EnrichmentRequest()
        return self._request

    def perform(self):
        super(EnrichmentAction, self).perform()
