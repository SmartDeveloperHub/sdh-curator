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

import os
import imp
import inspect
from rdflib import Graph
import sys
import itertools

__author__ = 'Fernando Serena'


def search_module(file_path, predicate, limit=1):
    mod_name, file_ext = os.path.splitext(os.path.split(file_path)[-1])
    py_mod = None

    if file_ext.lower() == '.py':
        py_mod = imp.load_source(mod_name, file_path)

    elif file_ext.lower() == '.pyc':
        py_mod = imp.load_compiled(mod_name, file_path)

    if py_mod is not None:
        cand_elms = filter(predicate, inspect.getmembers(py_mod, inspect.isclass))
        if len(cand_elms) > limit:
            raise ValueError('Too many elements in module {}'.format(mod_name))
        return cand_elms

    return None


class CGraph(Graph):
    def objects(self, subject=None, predicate=None, card=None):
        objs_gen = super(CGraph, self).objects(subject, predicate)
        if card is None:
            return objs_gen

        objs_gen, gen_bak = itertools.tee(objs_gen)
        objs = list(objs_gen)

        if card == 1:
            if not (0 < len(objs) < 2):
                raise ValueError(len(objs))
            return objs.pop()

        return gen_bak

    def subjects(self, predicate=None, object=None, card=None):
        subs_gen = super(CGraph, self).subjects(predicate, object)
        if card is None:
            return subs_gen

        subs_gen, gen_bak = itertools.tee(subs_gen)
        subs = list(subs_gen)

        if card == 1:
            if not (0 < len(subs) < 2):
                raise ValueError(len(subs))
            return subs.pop()

        return gen_bak
