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
from hdfs3_ext import HDFileSystemExt


supported_codecs = ('big5', 'gbk', 'gb2312', 'gb18030', 'utf_8')


def usage():
    print "hdfs_put.py [options] src dest"
    print "  -f encoding: src file's encoding, supported codecs '%s'" % ' '.join(supported_codecs)
    print "  -t encoding: dest file's encoding, supported codecs '%s'" % ' '.join(supported_codecs)
    print "  -r 'regex patten': regex to be applied on the src file, like '/\\x1E/\\x01/;/\\x0C//;/\\x0D//;/\\x00//'"


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")
    
    logging.basicConfig(level=logging.DEBUG, format="%(message)s", filename=os.path.join(mydir(), "hdfs_put.log"), filemode="w")
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    console.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger('').addHandler(console)

    parser = OptionParser()
    parser.add_option("-f", action="store", type="string", dest="from_encoding", help="-f encoding: src file's encoding")
    parser.add_option("-t", action="store", type="string", dest="to_encoding", help="-t: dest file's encoding")
    parser.add_option("-r", action="store", type="string", dest="regex", help="-r 'regex patten': regex to be applied on the src file")
    options, args = parser.parse_args()

    from_encoding, to_encoding, regex = None, None, None

    if options.from_encoding is not None:
        if options.from_encoding not in supported_codecs:
            usage()
            exit(128)
        else:
            from_encoding = options.from_encoding

    if options.to_encoding is not None:
        if options.to_encoding not in supported_codecs:
            usage()
            exit(128)
        else:
            to_encoding = options.to_encoding

    if options.regex is not None:
        regex = options.regex

    if len(args) != 2:
        usage()
        exit(128)
    src = args[0:-1]
    dest = args[-1]
    logging.debug("src=%s" % repr(src))
    logging.debug("dest=%s" % repr(dest))
    if not dest.startswith('/'):
        usage()
        exit(128)

    try:
        hdfs = HDFileSystemExt()
        logging.debug(repr(hdfs.conf))
        hdfs.put_with_conversion(src[0], dest, from_encoding, to_encoding, regex)
    except:
        logging.error("failed to put file to HDFS due to:")
        trace_log()
        exit(128)



