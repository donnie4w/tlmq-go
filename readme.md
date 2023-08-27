### mq client for tldb in go

------------

See the example at  http://tlnet.top/tlmq

```go
新建mq客户端实例
sc := cli.SimpleClient{Url: "ws://127.0.0.1:5100", Auth: "mymq=123"}

调用Connect()函数连接服务器
sc.Connect()

SimpleClient结构体中
1.Url 为websocket 的访问连接，如：ws://127.0.0.1:5100
若服务器启动TLS安全链接，则Url应为：wss://127.0.0.1:5100
2.Auth 为mq链接用户名密码，用等于号连接起来
3.Origin 为 http请求头origin,默认值:http://tldb-mq

服务器向MQ客户端发送的数据会分发到以下函数
1.PullByteHandler 客户端拉取数据异步返回 *MqBean 数据对象
2.PullJsonHandler  客户端拉取数据异步返回 *JMqBean 数据对象
3.PubByteHandler 客户端接收其他PubByte发布的 *MqBean 数据对象
4.PubJsonHandler 客户端接收其他PubJson发布的 *JMqBean 数据对象
5.PubMemHandler 客户端接收其他PubMem发布的 *MqBean 内存数据对象
6.AckHandler 服务器对客户端发送的每条数据的ACK回执
7.ErrHandler  服务器返回请求处理错误的错误码；如1301用户名密码错误

PubByte发布的数据，将由PubByteHandler 接收处理
PubJson发布的数据，将由PubJsonHandler 接收处理
PubMem发布的数据，将由PubMemHandler 接收处理


各个处理函数具体实现
sc.PubByteHandler(func(mb *MqBean) { logging.Debug("PubByte >> ", mb) })
sc.PubJsonHandler(func(jmb *JMqBean) { logging.Debug("PubJson >> ", jmb) })
sc.PubMemHandler(func(jmb *JMqBean) { logging.Debug("PubMem >> ", jmb) })
sc.PullByteHandler(func(mb *MqBean) { logging.Debug("PullByte >> ", mb) })
sc.PullJsonHandler(func(jmb *JMqBean) { logging.Debug("PullJson >> ", jmb) })
sc.AckHandler(func(id int64) { logging.Debug("ack >> ", id) })
sc.ErrHandler(func(code int64) { logging.Error("err code >> ", code) })


实际使用中，只需实现需要处理的函数，其他函数可无需实现；如：
sc.PubByteHandler(func(mb *MqBean) { logging.Debug("PubByte >> ", mb) })
订阅sc.Sub("usertable")
所有执行sc.PubByte("usertable", []byte("this is go usertable byte >>"))发布的数据均会发送到 PubByteHandler 函数中

订阅topic
sc.Sub("usertable")
删除订阅
sc.SubCancel("usertable")

发布topic
1.sc.PubByte("usertable", []byte("this is go usertable byte"))
发布数据由PubByteHandler函数接收处理

2.sc.PubJson("usertable", []byte("this is go usertable json"))
发布数据由PubJsonHandler函数接收处理

3.sc.PubMem("usertable", []byte("this is go usertable mem"))
发布数据由PubMemHandler函数接收处理
该接口不存数据，所以JMqBean中ID为0

异步拉取数据
sc.PullByte("usertable",1)
sc.PullJson("usertable",2)
PullByte拉取数据由 PullByteHandler函数接收处理
PullJson拉取数据由 PullJsonHandler函数接收处理

拉取topic的当前ID值，如：
sc.PullIdSync("usertable")
PullIdSync 拉取 topic 的当前ID值

同步拉取数据
mb,err:=sc.PullByteSync("usertable",1)
jmb,err:=sc.PullJsonSync("usertable",2)


聚合接收数据
sc.MergeOn(10)
参数10表示10M,设定服务器发送协议数据压缩前大小上限

启动客户端回执
sc.RecvAckOn(60)
客户端ack确认收到信息，否则服务器不认为信息已送达，将一直发送, 60s 设定服务器重发时间
```