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
	subMap    map[string]byte
	//接收服务器数据
	PullByteHandler func(mb *MqBean)
	PullJsonHandler func(jmb *JMqBean)
	PubByteHandler  func(mb *MqBean)
	PubJsonHandler  func(jmb *JMqBean)
	PubMemHandler   func(jmb *JMqBean)
	AckHandler      func(id int64)
	ErrHandler      func(code int64)
}

func (this *SimpleClient) Connect() (err error) {
	this.pingCount = 0
	this.conf = &Config{}
	this.conf.Url = this.Url + "/mq"
	this.conf.Origin = this.Origin
	this.conf.Auth = this.Auth
	//出错后关闭连接并重新连接
	this.conf.OnError = func(c *Cli, err error) {
		logging.Error("OnError")
		time.Sleep(1 * time.Second)
		this.MqCli.Close()
		this.Connect()
	}

	//处理服务器信息
	this.conf.OnMessage = func(c *Cli, msg []byte) {
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
	if this.subMap != nil {
		for k := range this.subMap {
			this.Sub(k)
		}
	}
	return
}

func (this *SimpleClient) doMsg(msg []byte) {
	ty := msg[0]
	switch ty {
	case MQ_PUBBYTE:
		if mb, err := TDecode(msg[1:], &MqBean{}); err == nil && this.PubByteHandler != nil {
			go this.PubByteHandler(mb)
		}
	case MQ_PULLBYTE:
		if mb, err := TDecode(msg[1:], &MqBean{}); err == nil && this.PullByteHandler != nil {
			go this.PullByteHandler(mb)
		}
	case MQ_PUBJSON:
		if mb, err := JDecode(msg[1:]); err == nil && this.PubJsonHandler != nil {
			this.PubJsonHandler(mb)
		}
	case MQ_PUBMEM:
		if mb, err := JDecode(msg[1:]); err == nil && this.PubMemHandler != nil {
			this.PubMemHandler(mb)
		}
	case MQ_PULLJSON:
		if mb, err := JDecode(msg[1:]); err == nil && this.PullJsonHandler != nil {
			this.PullJsonHandler(mb)
		}
	case MQ_PING:
		this.pingCount--
	case MQ_MERGE:
		if _r, err := ZlibUnCz(msg[1:]); err == nil {
			logging.Info("merge >>", len(msg[1:]), " >> ", len(_r))
			if mb, err := TDecode(_r, &MergeBean{}); err == nil {
				for _, bl := range mb.BeanList {
					this.doMsg(bl)
				}
				if len(mb.BeanList) == 0 {
					logging.Error("len(mb.BeanList) == 0 ")
				}
			} else {
				logging.Error("TDecode err >> ", err)
			}
		} else {
			logging.Error("err >> ", err)
		}
	case MQ_ACK:
		if r, err := BytesToInt64(msg[1:]); err == nil {
			if this.AckHandler != nil {
				this.AckHandler(r)
			}
		}
	case MQ_ERROR:
		if r, err := BytesToInt64(msg[1:]); err == nil {
			if this.ErrHandler != nil {
				this.ErrHandler(r)
			}
		}
	}
}

// 每3秒ping一次服务器，出错后超过次数时，关闭连接
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

func (this *SimpleClient) Sub(topic string) (_r int64, err error) {
	if this.subMap == nil {
		this.subMap = make(map[string]byte, 0)
	}
	this.subMap[topic] = 0
	return this.MqCli.Sub(topic)
}

func (this *SimpleClient) SubCancel(topic string) (_r int64, err error) {
	if this.subMap != nil {
		delete(this.subMap, topic)
	}
	return this.MqCli.SubCancel(topic)
}

func (this *SimpleClient) PubByte(topic string, msg []byte) (_r int64, err error) {
	return this.MqCli.PubByte(topic, msg)
}

func (this *SimpleClient) PubJson(topic string, msg string) (_r int64, err error) {
	return this.MqCli.PubJson(topic, msg)
}

func (this *SimpleClient) PubMem(topic string, msg string) (_r int64, err error) {
	return this.MqCli.PubMem(topic, msg)
}

func (this *SimpleClient) PullByte(topic string, id int64) (_r int64, err error) {
	return this.MqCli.PullByte(topic, id)
}

func (this *SimpleClient) PullJson(topic string, id int64) (_r int64, err error) {
	return this.MqCli.PullJson(topic, id)
}

func (this *SimpleClient) PullByteSync(topic string, id int64) (mb *MqBean, err error) {
	return this.MqCli.PullByteSync(topic, id)
}

func (this *SimpleClient) PullJsonSync(topic string, id int64) (jmb *JMqBean, err error) {
	return this.MqCli.PullJsonSync(topic, id)
}

func (this *SimpleClient) RecvAckOn(sec int8) (_r int64, err error) {
	return this.MqCli.RecvAckOn(sec)
}

func (this *SimpleClient) MergeOn(size int8) (_r int64, err error) {
	return this.MqCli.MergeOn(size)
}
