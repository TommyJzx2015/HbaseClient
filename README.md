# hbase client 运维工具
## 1. 背景
考虑到运维hbase集群时会有需要查看hbase表数据，
hbase shell 查询出来的value没有tostring, 经常乱码, 
merge 小region时经常需要 频繁查看region大小执行merge命令，效率比较低，特此做此工具
## 2. 用法
```
支持的操作有get, scan， merge
show_usage="args:
-a --action=[list/put/get/scan/merge/alter/major_compact/truncate/truncate_preserve],
-n --namespace=]
-t --tablename=]
-s --startrow=row], get action must have startrow, scan action have default value __allRows__
                  when action=alter,  startrow will be CONFIGURATION,
                  when action=put, startrow can be one rowkey
-e --endrow=row], scan action have default value __allRows__
                  when action=alter, and with columnfamily value, endrow will be columnfamily attributes name,for example TTL,COMPRESSION,DATA_BLOCK_ENCODING
-c --columnfamily=cf,
-f --column=,
-i --limit=, when action=scan, the num rows of scan action
            when action=alter, limit will be attributes value
            when action=put, outfile is null, limit will be cell value
-o --outfile, when action=get/scan/list, the result action will output to this file, default is ./logs/hclient.data
            when action=put, outfile is not null,will read from outfile data put into hbase
-l --lowsizeperregion, the max hfile size of per region joined in merge group
-u --upperregionsize, the max hfile size of new region after merge
-m --maxregiongroupsize, when action=merge, the num regions of merge group
                         when action=put, maxregiongroupsize can be puts group size
-h --help
```
### 2.1 scan 
```
   查看 limit 10 行数据
   sh  hclient.sh -a scan -t ATLAS_ENTITY_AUDIT_EVENTS -i 10
   
   指定startkey, 列族，列名，并发挥limit 10 行数
   sh  hclient.sh -a scan -t HBASE_TABLE -s 0173 -c f -f user_id,off -i 10
```
### 2.2 get
```
   指定key 查询
   sh  hclient.sh -a get -t HBASE_TABLE -s 0173df2a8basd
```

### 2.3 merge
```
   merge 合并测试，常用于merge 操作前测试
   将hfile 小于500MB, 单次合并region个数不超过2个，合并region后的hfile大小不会超过2048MB
   sh hclient.sh -a Tmerge -t HBASE_TABLE -l 500 -u 2048 -m 2

   merge操作
   将hfile 小于500MB, 单次合并region个数不超过2个，合并region后的hfile大小不会超过2048MB
   sh hclient.sh -a merge -t HBASE_TABLE -l 500 -u 2048 -m 2
```

### 2.4 alter
```
   修改表列族属性
   sh hclient.sh  -a alter -t METRIC_AGGREGATE_HOURLY_UUID -c 0 -e TTL,DATA_BLOCK_ENCODING,COMPRESSION    -i 5184000,FAST_DIFF,SNAPPY
   
```
### 2.5 list
```
    查看所有表
    sh hclient.sh  -a list
```

### 2.6 put
```
    put 一行数据
    sh hclient.sh  -a put -t HBASE_TABLE -s a -c cf -f f1 -i test
    
    从./log/data 中批量读取put 数据，每10个put为一组，格式为
    row1,cf:f1,v1
    row2,cf:f2,v2
    
    sh hclient.sh  -a put -t HBASE_TABLE -o ./log/data -m 10
```