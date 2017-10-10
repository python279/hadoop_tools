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
import ConfigParser
from datetime import datetime
import logging
import logging.handlers
from optparse import OptionParser
from error import trace_msg, trace_log
from mydir import mydir


def hdfs_ls(path):
    tmp = tempfile.mktemp()
    with open(tmp, 'w') as f:
        try:
            subprocess.check_call(["hdfs", "dfs", "-ls", "-R", "-q", "-h", path], stdout=f, stderr=subprocess.PIPE)
        except:
            trace_log()
        return tmp

def hdfs_rm(path):
    try:
        return subprocess.check_call(["hdfs", "dfs", "-rm", path, "2>/dev/null"], shell=True)
    except:
        logging.error("failed to delete " + path + " due to:")
        trace_log()
        return 128

def usage():
    print "hdfs_find.py"
    print "  --ctime n: File status was last changed n*24 hours ago"
    print "  --path: Path to walk through"
    print "  --action: Action to do for the found files, 'print' or 'delete', default is 'print'"
    print "  --empty: Remove empty folder"


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")
	
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    trfh = logging.handlers.TimedRotatingFileHandler(os.path.join(mydir(), "hdfs_find.log"), "d", 1, 10)
    trfh.setLevel(logging.INFO)
    logging.getLogger().addHandler(trfh)

    parser = OptionParser()
    parser.add_option("--ctime", type="string", dest="ctime", help="--ctime n: File status was last changed n*24 hours ago")
    parser.add_option("--path", type="string", dest="path", help="--path: Path to walk through")
    parser.add_option("--action", type="string", dest="action", default="print", help="--action: Action to do for the found files, print or delete")
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
    delete = True if options.action is "delete" else False
    
    # first remove all expired
    tmp = hdfs_ls(options.path)
    with open(tmp, 'r') as f:
        now = datetime.now()
        file_info = re.compile('^([^ ]+) +([\d-]+) +([^ ]+) +([^ ]+) +([^ ]+ *\w{0,1}) +(\d{4}-\d{2}-\d{2} \d{2}:\d{2}) +([^ ]+)')
        file_list = []
        
        # first remove expired files
        for line in f:
            try:
                m = file_info.match(line)
                permission, replication, userid, groupid, size, date_time, path = m.groups()
            except:
                logging.error("failed nto find file info pattern from:")
                logging.error(line)
                continue
            if not permission.startswith('d'):
                td = now - datetime.strptime(date_time, "%Y-%m-%d %H:%M")
                if td.days >= n:
                    print "%s %s" % (date_time, path)
                    if delete:
                        hdfs_rm(path)
                    continue
            # leave the un-deleted files and folders
            file_list.append(line)
        
        # second remove expired and empty folders
        
    os.remove(tmp)


