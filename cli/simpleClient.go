/**
 * Copyright 2023 tldb Author. All Rights Reserved.
 * email: donnie4w@gmail.com
 */

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

	//接收服务器数据
	PullByteHandler func(mb *MqBean)
	PullJsonHandler func(jmb *JMqBean)
	PubByteHandler  func(mb *MqBean)
	PubJsonHandler  func(jmb *JMqBean)
	AckHandler      func(id int64)
	ErrHandler      func(code int64)
}

func (this *SimpleClient) Connect() (err error) {
	this.pingCount = 0
	this.conf = &Config{}
	this.conf.Url = this.Url
	this.conf.Origin = this.Origin

	//出错后关闭连接并重新连接
	this.conf.OnError = func(c *Cli, err error) {
		logging.Error("OnError")
		time.Sleep(1 * time.Second)
		this.MqCli.Close()
		this.Connect()
	}

	//处理服务器信息
	this.conf.OnMessage = func(c *Cli, msg []byte) {
		switch msg[0] {
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
		case MQ_PULLJSON:
			if mb, err := JDecode(msg[1:]); err == nil && this.PullJsonHandler != nil {
				this.PullJsonHandler(mb)
			}
		case MQ_PING:
			this.pingCount--
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
	if this.MqCli, err = NewCli(this.conf); err == nil {
		logging.Debug("newCli success")
		this.MqCli.Auth(this.Auth)
		go this.ping()
	} else {
		time.Sleep(1 * time.Second)
		logging.Debug("reconn")
		this.Connect()
	}
	return
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
