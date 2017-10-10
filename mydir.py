# -*- coding: utf-8 -*-
#
# lhq
#


import os
import inspect


def mydir():
    this_file = inspect.getfile(inspect.currentframe())
    d = os.path.abspath(os.path.dirname(this_file))
    return d
