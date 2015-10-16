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

__author__ = 'Fernando Serena'

import logging


class Config(object):
    STORE_PATHS = {
        'graph': 'graph_store'
    }
    PORT = 5002
    RABBIT = {
        'host': 'localhost',
        'port': 5672
    }


class DevelopmentConfig(Config):
    DEBUG = True
    LOG = logging.DEBUG
    REDIS = {
        'host': 'localhost',
        'db': 4,
        'port': 6379
    }
    STORE = 'memory'
    AGORA = 'http://localhost:9001'


class TestingConfig(Config):
    DEBUG = False
    LOG = logging.DEBUG
    REDIS = {
        'host': 'localhost',
        'db': 5,
        'port': 6379
    }
    TESTING = True
    STORE = 'memory'


class ProductionConfig(Config):
    DEBUG = False
    LOG = logging.INFO
    REDIS = {
        'host': 'redis',
        'db': 4,
        'port': 6379
    }
    STORE = 'persist'
