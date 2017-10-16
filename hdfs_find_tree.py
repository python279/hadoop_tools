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


class FileInfo(NodeMixin):
    def __init__(self, name, date_time, folder, file_path, parent=None):
        super(FileInfo, self).__init__()
        self.name = name
        self.date_time = date_time
        self.folder = folder
        self.file_path = file_path
        self.parent = parent

def hdfs_ls(path):
    tmp = tempfile.mktemp()
    with open(tmp, 'w') as f:
        try:
            subprocess.check_call(["hdfs", "dfs", "-ls", "-R", "-q", "-h", path], stdout=f, stderr=subprocess.PIPE)
        except:
            trace_log()
        return tmp

def hdfs_rm(path, is_folder):
    try:
        if is_folder:
            return subprocess.check_call(["hdfs", "dfs", "-rmdir", "--ignore-fail-on-non-empty", path], stderr=subprocess.PIPE)
        else:
            return subprocess.check_call(["hdfs", "dfs", "-rm", "-f", path], stderr=subprocess.PIPE)
    except:
        logging.error("failed to delete " + path + " due to:")
        trace_log()
        return 128

def usage():
    print "hdfs_find_tree.py"
    print "  --ctime n: File status was last changed n*24 hours ago"
    print "  --path: Path to walk through"
    print "  --action: Action to do for the found files, 'print' or 'delete', default is 'print'"
    print "  --empty: Remove empty folder"


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")
    
    logging.basicConfig(level=logging.DEBUG, format="%(message)s", filename=os.path.join(mydir(), "hdfs_find_tree.log"), filemode="w")
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    console.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger('').addHandler(console)

    parser = OptionParser()
    parser.add_option("--ctime", action="store", type="string", dest="ctime", help="--ctime n: File status was last changed n*24 hours ago")
    parser.add_option("--path", action="store", type="string", dest="path", help="--path: Path to walk through")
    parser.add_option("--action", action="store", type="string", dest="action", default="print", help="--action: Action to do for the found files, print or delete")
    parser.add_option("--empty", action="store_true", dest="empty", default=False, help="--empty: Remove empty folder")
    options, args = parser.parse_args()
    
    if None in (options.path, options.ctime, options.action):
        usage()
        exit(128)
    if options.action not in ("print", "delete"):
        usage()
        exit(128)
    try:
        n = int(options.ctime)
    except:
        usage()
        exit(128)
    delete = True if options.action == "delete" else False

    # trim the right last '/'
    options.path = options.path.rstrip('/')
    tmp = hdfs_ls(options.path)
    with open(tmp, 'r') as f:
        now = datetime.now()
        file_info = re.compile('^([^ ]+) +([\d-]+) +([^ ]+) +([^ ]+) +([^ ]+ *\w{0,1}) +(\d{4}-\d{2}-\d{2} \d{2}:\d{2}) +(.+)$')
        
        # first build the tree from the file list
        root = FileInfo(os.path.basename(options.path), None, True, options.path, None)
        pre_node = root
        for line in f:
            try:
                m = file_info.match(line)
                permission, replication, userid, groupid, size, date_time, path = m.groups()
            except:
                logging.error("failed to find file info pattern from:")
                logging.error(line)
                continue
            if pre_node.file_path == os.path.dirname(path):
                new_node = FileInfo(os.path.basename(path), date_time, permission.startswith('d'), path, pre_node)
                logging.debug("new_node: name=%s, date_time=%s, file_path=%s, parent=%s" % (new_node.name, new_node.date_time, new_node.file_path, pre_node.file_path))
            else:
                while True:
                    pre_node = pre_node.parent
                    if pre_node is None:
                        raise Exception("oops: sub node has no parent <%s>, ignore" % path)
                    if pre_node.file_path == os.path.dirname(path):
                        new_node = FileInfo(os.path.basename(path), date_time, permission.startswith('d'), path, pre_node)
                        logging.debug("new_node: name=%s, date_time=%s, file_path=%s, parent=%s" % (new_node.name, new_node.date_time, new_node.file_path, pre_node.file_path))
                        break
            pre_node = new_node
        logging.info(RenderTree(root, style=AsciiStyle()).by_attr())
        
        # then do post order iterator with the tree, delete expired files and then the empty directory
        for node in PostOrderIter(root):
            if node.is_leaf:
                if node.folder and not options.empty:
                    continue
                td = now - datetime.strptime(node.date_time, "%Y-%m-%d %H:%M")
                if td.days >= n:
                    if delete:
                        print "delete %s %s" % (node.date_time, node.file_path)
                        hdfs_rm(node.file_path, node.folder)
                    else:
                        print "%s %s" % (node.date_time, node.file_path)
                    node.parent = None

    # last remove the temp file
    os.remove(tmp)
    exit(0)


