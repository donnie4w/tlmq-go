/**
* Copyright 2023 tldb Author. All Rights Reserved.
* email: donnie4w@gmail.com
* https://github.com/donnie4w/tldb
* https://github.com/donnie4w/tlmq-go
**/

package cli

import (
	"time"

	"github.com/donnie4w/simplelog/logging"
	. "github.com/donnie4w/tlmq-go/stub"
)

type SimpleClient struct {
	Url       string
	Origin    string
	Auth      string
	pingCount int
	MqCli     *Cli
	conf      *Config
	zlib      bool
	subMap    map[string]byte
	//接收服务器数据
	pullByteHandler func(mb *MqBean)
	pullJsonHandler func(jmb *JMqBean)
	pubByteHandler  func(mb *MqBean)
	pubJsonHandler  func(jmb *JMqBean)
	pubMemHandler   func(jmb *JMqBean)
	ackHandler      func(id int64)
	errHandler      func(code int64)

	before func()
}

func NewMqClient(addr, auth string) MqClient {
	sc := &SimpleClient{Url: addr, Auth: auth}
	return sc
}

func (this *SimpleClient) Connect() (err error) {
	this.pingCount = 0
	this.conf = &Config{}
	this.conf.Url = this.Url + "/mq"
	this.conf.Origin = this.Origin
	this.conf.Auth = this.Auth
	//出错后关闭连接并重新连接
	this.conf.OnError = func(_ *Cli, _ error) {
		logging.Error("OnError")
		time.Sleep(1 * time.Second)
		this.MqCli.Close()
		this.Connect()
	}

	//处理服务器信息
	this.conf.OnMessage = func(_ *Cli, msg []byte) {
		ty := msg[0]
		if this.conf.RecvAckOn && (ty == MQ_PULLBYTE || ty == MQ_PULLJSON || ty == MQ_PUBJSON || ty == MQ_PUBBYTE || ty == MQ_MERGE) {
			this.MqCli.ackMsg(msg)
		}
		this.doMsg(msg)
	}

	if this.MqCli, err = NewCli(this.conf); err == nil {
		this.MqCli.Auth(this.conf.Auth)
		go this.ping()
	} else {
		<-time.After(time.Second)
		logging.Debug("reconn")
		this.Connect()
	}
	<-time.After(time.Second)
	if this.subMap != nil {
		for k := range this.subMap {
			this.Sub(k)
		}
	}
	if this.before != nil {
		this.before()
	}
	return
}

func (this *SimpleClient) doMsg(msg []byte) {
	ty := msg[0]
	switch ty {
	case MQ_PUBBYTE:
		if mb, err := TDecode(msg[1:], &MqBean{}); err == nil && this.pubByteHandler != nil {
			go this.pubByteHandler(mb)
		}
	case MQ_PULLBYTE:
		if mb, err := TDecode(msg[1:], &MqBean{}); err == nil && this.pullByteHandler != nil {
			go this.pullByteHandler(mb)
		}
	case MQ_PUBJSON:
		if mb, err := JDecode(msg[1:]); err == nil && this.pubJsonHandler != nil {
			this.pubJsonHandler(mb)
		}
	case MQ_PUBMEM:
		if mb, err := JDecode(msg[1:]); err == nil && this.pubMemHandler != nil {
			this.pubMemHandler(mb)
		}
	case MQ_PULLJSON:
		if mb, err := JDecode(msg[1:]); err == nil && this.pullJsonHandler != nil {
			this.pullJsonHandler(mb)
		}
	case MQ_PING:
		this.pingCount--
	case MQ_MERGE:
		var bs []byte
		var err error
		if this.zlib {
			bs, err = ZlibUnCz(msg[1:])
		} else {
			bs = msg[1:]
		}
		if err == nil {
			var mb *MergeBean
			if mb, err = TDecode(bs, &MergeBean{}); err == nil {
				for _, bl := range mb.BeanList {
					this.doMsg(bl)
				}
			}
		}
		if err != nil {
			logging.Error(err)
		}
	case MQ_ACK:
		if r, err := BytesToInt64(msg[1:]); err == nil {
			if this.ackHandler != nil {
				this.ackHandler(r)
			}
		}
	case MQ_ERROR:
		if r, err := BytesToInt64(msg[1:]); err == nil {
			if this.errHandler != nil {
				this.errHandler(r)
			}
		}
	}
}

// ping the server every 3 seconds. Close the connection if the number of errors exceeds
func (this *SimpleClient) ping() {
	defer _recover()
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ticker.C:
			if _, err := this.MqCli.Ping(); err != nil || this.pingCount > 3 {
				this.pingCount++
				this.MqCli.Close()
				goto END
			}
		}
	}
END:
}

// Subscribe to a topic
func (this *SimpleClient) Sub(topic string) (_r int64, err error) {
	if this.subMap == nil {
		this.subMap = make(map[string]byte, 0)
	}
	this.subMap[topic] = 0
	return this.MqCli.Sub(topic)
}

// Subscribe to a topic
func (this *SimpleClient) SubJson(topic string) (_r int64, err error) {
	if this.subMap == nil {
		this.subMap = make(map[string]byte, 0)
	}
	this.subMap[topic] = 0
	return this.MqCli.SubJson(topic)
}

// Unsubscribed topic
func (this *SimpleClient) SubCancel(topic string) (_r int64, err error) {
	if this.subMap != nil {
		delete(this.subMap, topic)
	}
	return this.MqCli.SubCancel(topic)
}

// Publishing topic and PubByteHandler will receive it
func (this *SimpleClient) PubByte(topic string, msg []byte) (_r int64, err error) {
	return this.MqCli.PubByte(topic, msg)
}

// Publishing topic and PubJsonHandler will receive it
func (this *SimpleClient) PubJson(topic string, msg string) (_r int64, err error) {
	return this.MqCli.PubJson(topic, msg)
}

// the topic body is not stored when use PubMem
func (this *SimpleClient) PubMem(topic string, msg string) (_r int64, err error) {
	return this.MqCli.PubMem(topic, msg)
}

// pull the topic body by topic id used asynchronization mode
func (this *SimpleClient) PullByte(topic string, id int64) (_r int64, err error) {
	return this.MqCli.PullByte(topic, id)
}

// pull the topic body by topic id used asynchronization mode
func (this *SimpleClient) PullJson(topic string, id int64) (_r int64, err error) {
	return this.MqCli.PullJson(topic, id)
}

// pull the topic body by topic id used synchronization mode
func (this *SimpleClient) PullByteSync(topic string, id int64) (mb *MqBean, err error) {
	return this.MqCli.PullByteSync(topic, id)
}

// pull the topic body by topic id used synchronization mode
func (this *SimpleClient) PullJsonSync(topic string, id int64) (jmb *JMqBean, err error) {
	return this.MqCli.PullJsonSync(topic, id)
}

// pull the maximum id number of the topic
func (this *SimpleClient) PullIdSync(topic string) (id int64, err error) {
	return this.MqCli.PullIdSync(topic)
}

// setup requires a client return receipt
func (this *SimpleClient) RecvAckOn(sec int8) (_r int64, err error) {
	return this.MqCli.RecvAckOn(sec)
}

// set the limit of the size of protocol data sent by the server before compression(Unit:MB)
func (this *SimpleClient) MergeOn(size int8) (_r int64, err error) {
	return this.MqCli.MergeOn(size)
}

func (this *SimpleClient) SetZlib(on bool) (_r int64, err error) {
	if _r, err = this.MqCli.SetZlib(on); err == nil {
		this.zlib = on
	}
	return
}

func (this *SimpleClient) PullByteHandler(f func(mb *MqBean)) {
	this.pullByteHandler = f
}
func (this *SimpleClient) PullJsonHandler(f func(jmb *JMqBean)) {
	this.pullJsonHandler = f
}
func (this *SimpleClient) PubByteHandler(f func(mb *MqBean)) {
	this.pubByteHandler = f
}
func (this *SimpleClient) PubJsonHandler(f func(jmb *JMqBean)) {
	this.pubJsonHandler = f
}
func (this *SimpleClient) PubMemHandler(f func(jmb *JMqBean)) {
	this.pubMemHandler = f
}
func (this *SimpleClient) AckHandler(f func(id int64)) {
	this.ackHandler = f
}
func (this *SimpleClient) ErrHandler(f func(code int64)) {
	this.errHandler = f
}

// method after the connection successful
func (this *SimpleClient) Before(f func()) {
	this.before = f
}
