
## 1.5G 文件：

### 直接hdfs put
```
[hadoop@namenodea hadoop_tools]$ time hdfs dfs -put test_utf8.1.5G /tmp/test_utf8.1.5G

```

### 传统方式
```
[hadoop@namenodea hadoop_tools]$ time sh -c "iconv -c -f gb2312 -t utf-8 test_gb2312.1.5G | sed 's/\x1E/\x01/g;s/\x0C//g;s/\x0D//g;s/\x00//g' > test_utf8.1.5G; hdfs dfs -put test_utf8.1.5G /tmp/test_utf8.1.5G"
real 4m34.539s
user 1m24.360s
sys 0m14.020s
```

### native模式
```
[hadoop@namenodea hadoop_tools]$ time  python hdfs_put.py -f gb2312 -t utf_8 -r '/\x1E/\x01/;/\x0C//;/\x0D//;/\x00//' test_gb2312.1.5G /tmp/test_utf8.1.5G                                                   
real 4m15.839s
user 3m27.745s
sys 0m4.418s
```

### native模式（多线程优化）
```
[hadoop@namenodea hadoop_tools]$ time  python hdfs_put.py -f gb2312 -t utf_8 -r '/\x1E/\x01/;/\x0C//;/\x0D//;/\x00//' test_gb2312.1.5G /tmp/test_utf8.1.5G                                                   
real 2m33.546s
user 2m35.438s
sys 0m8.707s
```

## 15G 文件：

### 直接hdfs put
```
[hadoop@namenodea hadoop_tools]$ time hdfs dfs -put test_utf8.15G /tmp/test_utf8.15G.8
real 28m3.539s
user 1m19.842s
sys 0m54.868s
```

### 传统方式
```
[hadoop@namenodea hadoop_tools]$ time sh -c "iconv -c -f gb2312 -t utf-8 test_gb2312.15G | sed 's/\x1E/\x01/g;s/\x0C//g;s/\x0D//g;s/\x00//g' > test_utf8.15G; hdfs dfs -put test_utf8.15G /tmp/test_utf8.15G"
real 67m43.316s
user 12m49.350s
sys 2m29.112s
```

### native模式
```
[hadoop@namenodea hadoop_tools]$ time  python hdfs_put.py -f gb2312 -t utf_8 -r '/\x1E/\x01/;/\x0C//;/\x0D//;/\x00//' test_gb2312.15G /tmp/test_utf8.15G                                                   
real 45m9.781s
user 34m16.836s
sys 0m42.119s
```

### native模式（多线程优化）
```
[hadoop@namenodea hadoop_tools]$ time  python hdfs_put.py -f gb2312 -t utf_8 -r '/\x1E/\x01/;/\x0C//;/\x0D//;/\x00//' test_gb2312.15G /tmp/test_utf8.15G                                                   
real 36m26.896s
user 27m31.846s
sys 1m24.436s
```