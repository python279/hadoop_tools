#!/bin/env python
# -*- coding: utf-8 -*-
#
# lhq

import os
import re
import sys
import time
import tempfile
import subprocess
from datetime import datetime
import logging
import logging.handlers
from optparse import OptionParser
from error import trace_msg, trace_log
from mydir import mydir
from anytree import Node, NodeMixin, RenderTree, AsciiStyle
from anytree.iterators import PostOrderIter
import codecs
from hdfs3 import HDFileSystem
from hdfs3.utils import MyNone


class HDFileSystemExt(HDFileSystem):
    _handle = None
    _regex = None

    def __init__(self, host=MyNone, port=MyNone, connect=True, autoconf=True, pars=None, **kwargs):
        super(HDFileSystemExt, self).__init__(host, port, connect, autoconf, pars, *kwargs)

    @staticmethod
    def _regex_sub(regex, string):
        if regex:
            r = [g.strip('/').split('/') for g in regex.split(';')]
            # first do all replace='' replacement
            string = re.sub(r'[%s]' % ''.join([t[0] for t in r if len(t) == 1]), r'', string, re.U | re.M)
            # then do all other replacement
            for t in r:
                if len(t) == 2:
                    string = re.sub(r'[%s]' % t[0], r'%s' % t[1], string, re.U | re.M)
        return string

    @staticmethod
    def _string_transcoding(from_encoding, to_encoding, string):
        if None not in (from_encoding, to_encoding):
            string = string.encode(to_encoding)
        return string

    def put_with_conversion(self, src, dest, from_encoding=None, to_encoding=None, regex=None):
        with self.open(dest, 'wb') as f:
            with codecs.open(src, 'r', from_encoding) as f2:
                for line in f2:
                    out = self._string_transcoding(from_encoding, to_encoding, self._regex_sub(regex, line))
                    if len(out) == 0:
                        continue
                    f.write(out)
