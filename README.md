# hdfs_find_tree.py

## usage

```
hdfs_find_tree.py
  --ctime n: File status was last changed n*24 hours ago
  --path: Path to walk through
  --action: Action to do for the found files, 'print' or 'delete', default is 'print'
  --empty: Remove empty folder
```

## example

```
# find out all files and folders modified 90 days ago
hdfs_find_tree.py --ctime 90 --path /tmp/hive/hadoop --empty

# find out all files and folders modified 90 days ago and delete all of them and empty folders
hdfs_find_tree.py --ctime 90 --path /tmp/hive/hadoop --empty --action delete
```

## log

```
there are useful logs in hdfs_find_tree.log under current folder
```
