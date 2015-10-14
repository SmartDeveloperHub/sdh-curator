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

from sdh.curator.actions import Action
import logging
from rdflib import RDF
from rdflib.namespace import Namespace

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions.enrichment')
CURATOR = Namespace('http://www.smartdeveloperhub.org/vocabulary/curator#')


class EnrichmentAction(Action):
    def run(self):
        log.info('Received a potential enrichment request')
        request_nodes = list(self._request_graph.subjects(RDF.type, CURATOR.EnrichmentRequest))
        if not (0 < len(request_nodes) < 2):
            raise SyntaxError('There is no enrichment request specified in the message')

        request_node = request_nodes.pop()
        target_resource_nodes = list(self._request_graph.objects(request_node, CURATOR.targetResource))
        if not (0 < len(target_resource_nodes) < 2):
            raise SyntaxError('Invalid number of target resources: {}'.format(len(target_resource_nodes)))

        target_resource = target_resource_nodes.pop()
        log.debug('Found a request to enrich {}!'.format(target_resource))
