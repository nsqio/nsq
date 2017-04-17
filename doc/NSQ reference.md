# 新版使用指南

## 实现上值得一提的兼容性改动
### 内部ID
内部消息ID 使用16 字节存储, 但是具体的内容有所变动, 理论上外部客户端不依赖内部消息ID的具体内容, 因此标准的SDK可以兼容使用. 内部ID由原来的16字节的ascii hex string, 改变成了8字节的自增ID和8字节的跟踪ID.客户端的新功能和高级接口会使用跟踪ID的内容.
### 关于延迟消息
目前的延迟消息精度可能不准, 延迟功能应该理解为稍后消费, 不一定会按照指定的延迟时间推送(仅作为延迟的参考)
### topic区分读写请求
为了区分读写请求, 生产者SDK都会传入写入请求标记, 消费者都会传读标记. 这些标记用于返回不同状态的数据节点(可以写入的节点和可以读取的节点区分开). 老的客户端由于不支持读写参数, 因此都是返回所有的可用的topic数据节点.


## 此fork和原版的几点运维上的不同
### 关于topic的创建和删除
此版本为了内部的运维方便, 去掉了nsqd上的自动创建和删除topic的接口, 避免大量业务使用时创建的topic不在运维团队的管理范围之内, 因此把创建topic的API禁用了, 统一由运维通过nsqadmin创建需要的topic.

### 关于topic的分区限制
由于老版本没有完整的分区概念, 每台机子上面只能有一个同名的topic, 因此新版本为了兼容老版本的客户端, 对topic的partition数量做了限制, 每台机子每个topic只能有一个分区(含副本). 因此创建topic时的分区数*副本数应该不能大于集群的nsqd节点数. 对于顺序消费的topic无此限制, 因为老版本的客户端不支持顺序消费特性.

### 关于topic的初始化
由于topic本身是数据存储资源的单位, 为了避免垃圾资源, 运维限制没有初始化的topic是不能写入的. 没有初始化的topic表现为没有任何业务客户端创建channel. 因此一个没有任何channel的topic是不能写入数据的. 为了能完成初始化, 需要在nsqadmin界面创建一个默认的channel用于消费. 没有任何channel的topic如果允许写入, 会导致写入的数据没有任何人去消费, 导致磁盘一直增长.

### 关于顺序topic
顺序topic允许同一个节点存储多个分区, 创建是需要在api指定 `orderedmulti=true` 参数. 顺序topic不允许使用非顺序的方式进行消费. 因此老的官方客户端不能使用该功能


## 常用运维操作
### topic禁止某个分区节点写入 往所有的lookup节点发送如下命令
<pre>
/topic/tombstone?topic=xxx&node=ip:httpport
</pre>
重新允许写入 
<pre>
/topic/tombstone?topic=xxx&node=ip:httpport&restore=true
</pre>
此操作可以用于当某个节点磁盘不足时临时禁止写入.

### 动态调整服务端日志级别
<pre>
nsqd: curl -X POST "http://127.0.0.1:4151/loglevel/set?loglevel=3"
nsqlookupd: curl -X POST "http://127.0.0.1:4161/loglevel/set?loglevel=3"
</pre>
loglevel数字越大, 日志越详细

### 集群节点维护
以下几个API是nsqlookupd的HTTP接口
主动下线某个节点, 其中nodeid是分布式的id, 可以在nsqadmin里面查看对应节点的id, 调用后, 系统会自动将topic数据逐步平滑迁移到其他节点, 等待完成后, 运维就可以直接关机了. 此操作用于永久从集群中下线一台机子.
<pre>
/cluster/node/remove?remove_node=nodeid
</pre>

以下API可以用于改变topic的元数据信息, 支持修改副本数, 刷盘策略, 保留时间, 如果不需要改,可以不需要传对应的参数.
<pre>
/topic/meta/update?topic=xxx&replicator=xx&syncdisk=xx&retention=xxx
</pre>

### 消息跟踪
服务端可以针对topic动态启用跟踪, 远程的跟踪系统是内部使用的, 因此无法提供, 不过可以使用默认的log跟踪模块. 以下跟踪打开时, 会把跟踪信息写入log文件. 以下API发送给对应的nsqd节点.
<pre>
// 启用写入跟踪
$ curl -X POST "http://127.0.0.1:4151/message/trace/enable?topic=balance_test3"
// 跟踪指定channel的消费情况
$ curl -X POST "http://127.0.0.1:4151/message/trace/enable?topic=perf_2_2_5&channel=perf_2_2_5_ch0"
// 关闭消费跟踪
$ curl -X POST "http://127.0.0.1:4151/message/trace/disable?topic=perf_2_2_5&channel=perf_2_2_5_ch0"
// 关闭写入跟踪
$ curl -X POST "http://127.0.0.1:4151/message/trace/disable?topic=balance_test3"
</pre>

### 指定消费位置
发送给对应的nsqd节点, 如果多个分区需要设置, 则对不同分区发送多次
<pre>
curl -X POST -d "xxx:xxx" "http://127.0.0.1:4151/channel/setoffset?topic=xxx&channel=xxx"
POST body:
timestamp:xxxx  (指定消费时间起点seconds, 自1970-1-1开始的秒数)
或者
virtual_queue:xxx  (指定消费队列字节位置起点, 从队列头部开始计算)
或者
msgcount:xxx (指定消费消息条数起点,从队列头部开始计算)
</pre>