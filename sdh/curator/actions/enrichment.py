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

    def _extract_content(self):
        super(EnrichmentRequest, self)._extract_content()


class EnrichmentAction(FragmentAction):
    def request(self):
        if self._request is None:
            self._request = EnrichmentRequest()
        return self._request

    def perform(self):
        log.debug('Received a potential enrichment request')
        super(EnrichmentAction, self).perform()
