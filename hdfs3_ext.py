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
import cStringIO
import codecs
from multiprocessing import Process, Pipe
from hdfs3 import HDFileSystem
from hdfs3.utils import MyNone


class EOF(object):
    pass


class HDFileSystemExt(HDFileSystem):
    _handle = None

    def __init__(self, host=MyNone, port=MyNone, connect=True, autoconf=True, pars=None, **kwargs):
        super(HDFileSystemExt, self).__init__(host, port, connect, autoconf, pars, *kwargs)

    @staticmethod
    def _regex_sub(regex, string):
        def _hex_str_to_int(s):
            if s[0] == '\\' and s[1].lower() == 'x':
                return int('0' + s[2:], 16)
            else:
                return int(s)
        if regex:
            r = [g.strip('/').split('/') for g in regex.split(';')]
            # first do all replace='' replacement
            string = re.sub(r'[%s]' % ''.join([t[0] for t in r if len(t) == 1]), '', string, re.U | re.M)
            # then do all other replacement
            for t in r:
                if len(t) == 2:
                    string = re.sub(r'[%s]' % t[0], '%c' % _hex_str_to_int(t[1]), string, re.U | re.M)
        return string

    @staticmethod
    def _string_transcoding(from_encoding, to_encoding, string):
        if None not in (from_encoding, to_encoding):
            string = string.encode(to_encoding)
        return string

    def put_with_conversion(self, src, dest, from_encoding=None, to_encoding=None, regex=None):
        block_size = 64*2**20

        def _write(instance_context, dest, child_conn):
            self = instance_context
            with self.open(dest, 'wb') as f:
                fp = cStringIO.StringIO()
                buffer_len = 0
                while True:
                    data = child_conn.recv()
                    if isinstance(data, EOF):
                        # end of file, break
                        break
                    fp.write(data)
                    buffer_len += len(data)
                    if buffer_len >= block_size:
                        f.write(fp.getvalue())
                        fp.close()
                        buffer_len = 0
                        fp = cStringIO.StringIO()
                # write last segment
                if buffer_len:
                    f.write(fp.getvalue())
                    fp.close()

        parent_conn, child_conn = Pipe()
        child = Process(target=_write, args=(self, dest, child_conn))
        child.start()
        with codecs.open(src, 'r', from_encoding) as f2:
            fp = cStringIO.StringIO()
            buffer_len = 0
            for line in f2:
                out = self._string_transcoding(from_encoding, to_encoding, self._regex_sub(regex, line))
                if len(out) == 0:
                    continue
                fp.write(out)
                buffer_len += len(out)
                if buffer_len >= block_size:
                    parent_conn.send(fp.getvalue())
                    fp.close()
                    buffer_len = 0
                    fp = cStringIO.StringIO()
            # send last segment
            if buffer_len:
                parent_conn.send(fp.getvalue())
                fp.close()
            parent_conn.send(EOF())
            while child.is_alive():
                time.sleep(1)
            if child.exitcode != 0:
                raise Exception("child thread return non-zero value")
