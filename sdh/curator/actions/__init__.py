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
import pkgutil
import logging
import os
import imp
import inspect
from sdh.curator.actions.core.base import Action

__author__ = 'Fernando Serena'

log = logging.getLogger('sdh.curator.actions')
action_modules = {}
# def load_module(name):
#     (importer, name, _) = action_modules[name]
#     loader = importer.find_module(name)
#     file_path = loader.get_filename()
#     mod_name, file_ext = os.path.splitext(os.path.split(file_path)[-1])
#     py_mod = None
#     if file_ext.lower() == '.py':
#         py_mod = imp.load_source(mod_name, file_path)
#     elif file_ext.lower() == '.pyc':
#         py_mod = imp.load_compiled(mod_name, file_path)
#
#     action_modules[name] = py_mod

# action_modules = {x[1]: x for x in pkgutil.iter_modules(path=['sdh/curator/actions/ext'])}
# for module_name in action_modules:
#     load_module(module_name)

from sdh.curator.actions.ext import query, stream, enrichment

action_modules['query'] = query
action_modules['stream'] = stream
action_modules['enrichment'] = enrichment


def search_module(module, predicate, limit=1):
    py_mod = action_modules[module]

    if py_mod is not None:
        cand_elms = filter(predicate,
                           inspect.getmembers(py_mod, lambda x: inspect.isclass(x) and not inspect.isabstract(x)))
        if len(cand_elms) > limit:
            raise ValueError('Too many elements in module {}'.format(module))
        return cand_elms

    return None


def get_instance(module, clz, *args):
    module = action_modules[module]
    class_ = getattr(module, clz)
    instance = class_(*args)
    return instance


def execute(*args, **kwargs):
    log.debug('Searching for a compliant "{}" action handler...'.format(args[0]))
    name = args[0]

    try:
        _, clz = search_module(name,
                               lambda (_, cl): issubclass(cl, Action) and cl != Action).pop()
        data = kwargs.get('data', None)
        log.debug(
            'Found! Requesting an instance of {} to perform a/n {} action described as:\n{}'.format(clz, name, data))
        clz(data).submit()
    except IndexError:
        raise EnvironmentError('Action module found but class is missing: "{}"'.format(name))
