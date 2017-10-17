# hdfs_find_tree.py

## 用法
```
hdfs_find_tree.py
  --ctime n: 文件超过 n*24 小时没有修改过
  --path: 需要遍历的hdfs目录绝对路径
  --action: 对符合条件的目标的动作，“delete”或者”print”，不指定默认是”print”，建议删除之前先dryrun一下，确认程序没有误删别的文件
  --empty: 是否删除符合条件的空目录，不指定默认是False
```

## 例子
```
找出/tmp/hive/hadoop下面超过90天没有修改的文件和删除文件之后的空目录，只打印不删除
hdfs_find_tree.py --ctime 90 --path /tmp/hive/hadoop --empty

找出/tmp/hive/hadoop下面超过90天没有修改的文件，删除文件和删除文件之后的空目录
hdfs_find_tree.py --ctime 90 --path /tmp/hive/hadoop --empty --action delete

找出/tmp/hive/hadoop下面超过90天没有修改的文件，只删除文件，保留删除文件之后的空目录
注意：选择保留空目录会导致一个问题，一旦这个目录下面的文件有删除，此目录的日期会被改成当前时间，请根据实际情况使用此功能
hdfs_find_tree.py --ctime 90 --path /tmp/hive/hadoop --action delete
```

## 日志
```
当前目录下hdfs_find_tree.log包含一些有用的调式信息
```

## 缺点
```
如果文件比较多，调用hdfs dfs命令行删除文件比较慢，可以再优化，把删除文件和目录合并成一个hdfs dfs –rm的操作或者用java重写
```

## 原理
```
1.	hdfs dfs –ls –q –R –h获取文件目录列表
2.	对1的结果做遍历生成一颗树（树的实现用了anytree这个python插件）
3.	对2生成的树做深度后续遍历，先删叶子，再删父目录（如果空的话）
```

