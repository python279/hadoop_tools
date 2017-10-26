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
import glob
import logging
import logging.handlers
from tabulate import tabulate
from optparse import OptionParser
from error import trace_msg, trace_log
from mydir import mydir
from anytree import Node, NodeMixin, RenderTree, AsciiStyle
from anytree.iterators import PostOrderIter
from hdfs3_ext import *


supported_codecs = ('gbk', 'gb2312', 'gb18030', 'utf_8')


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
    console.setLevel(logging.INFO)
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
    src = args[0]
    dest = args[1]
    logging.debug("src=%s" % src)
    logging.debug("dest=%s" % dest)
    if not dest.startswith('/'):
        usage()
        exit(128)

    succeed = []
    failed = []
    try:
        hdfs = HDFileSystemExt()
        logging.debug(repr(hdfs.conf))

        # get all src, dest file list
        files_list = []
        if os.path.isdir(src):
            try:
                path_info = hdfs.info(dest)
            except FileNotFoundError:
                logging.error("dest dir(dest) does not exist, exit" % dest)
                exit(128)
            if path_info['kind'] != 'directory':
                logging.error("src(%s) is directory but dest(%s) is not, exit" % (src, dest))
                exit(128)
            logging.info("hdfs put from dir src(%s) to hdfs(%s)" % (src, dest))
            for src_file in glob.glob(os.path.join(src, '*')):
                if os.path.isfile(src_file):
                    dest_file = dest + os.path.basename(src_file) if dest.endswith('/') else dest + '/' + os.path.basename(src_file)
                    files_list.append((src_file,dest_file))
        else:
            try:
                path_info = hdfs.info(dest)
            except FileNotFoundError:
                path_info = None
                pass
            if path_info and path_info['kind'] == 'directory':
                dest_file = dest + os.path.basename(src) if dest.endswith('/') else dest + '/' + os.path.basename(src)
                files_list.append((src, dest_file))
            else:
                files_list.append((src, dest))
        hdfs.disconnect()

        # handle the src, dest in the files_list
        for src_file, dest_file in files_list:
            logging.info("hdfs put from src(%s) to hdfs(%s)" % (src_file, dest_file))
            try:
                hdfs = HDFileSystemExt()
                succeed.append(hdfs.put_with_conversion(src_file, dest_file, from_encoding, to_encoding, regex))
                hdfs.disconnect()
            except:
                failed.append([src_file, dest_file, "failed", None, None])
                logging.error("failed to put src(%s) to hdfs(%s) due to" % (src_file, dest_file))
                trace_log()

        # print out the result summary
        headers= ["src", "dest", "result", "total lines", "time(second)"]
        logging.info(tabulate(succeed+failed, headers, tablefmt="grid"))
    except:
        trace_log()
        exit(128)

