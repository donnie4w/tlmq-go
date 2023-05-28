/**
 * Copyright 2023 tldb Author. All Rights Reserved.
 * email: donnie4w@gmail.com
 */

package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/donnie4w/simplelog/logging"
	. "github.com/donnie4w/tlmq-go/stub"
	"golang.org/x/net/websocket"
)

const (
	MQ_AUTH     byte = 1
	MQ_PUBBYTE  byte = 2
	MQ_PUBJSON  byte = 3
	MQ_SUB      byte = 4
	MQ_PULLBYTE byte = 5
	MQ_PULLJSON byte = 6
	MQ_PING     byte = 7
	MQ_ERROR    byte = 8
	MQ_PUBMEM   byte = 9
	MQ_ACK      byte = 0
)

type Config struct {
	Url       string
	Origin    string
	Auth      string
	OnOpen    func(c *Cli)
	OnError   func(c *Cli, err error)
	OnClose   func(c *Cli)
	OnMessage func(c *Cli, msg []byte)
}

type Cli struct {
	conf     *Config
	conn     *websocket.Conn
	mux      *sync.Mutex
	_isError bool
}

func NewCli(conf *Config) (cli *Cli, err error) {
	var conn *websocket.Conn
	if conf.Origin == "" {
		conf.Origin = "http://tldb-mq"
	}
	if strings.HasPrefix(conf.Url, "wss:") {
		config := &websocket.Config{TlsConfig: &tls.Config{InsecureSkipVerify: true}, Version: websocket.ProtocolVersionHybi13}
		if config.Location, err = url.ParseRequestURI(conf.Url); err == nil {
			if config.Origin, err = url.ParseRequestURI(conf.Origin); err == nil {
				conn, err = websocket.DialConfig(config)
			}
		}
	} else {
		conn, err = websocket.Dial(conf.Url, "", conf.Origin)
	}
	if err == nil && conn != nil {
		cli = &Cli{conf, conn, &sync.Mutex{}, false}
		if conf.OnOpen != nil {
			conf.OnOpen(cli)
		}
		go cli._read()
	} else {
		logging.Error(err)
	}
	return
}

func (this *Cli) Ping() (_r int64, err error) {
	return this._sendMsg(MQ_PING, nil)
}

func (this *Cli) PubByte(topic string, msg []byte) (_r int64, err error) {
	return this._sendMsg(MQ_PUBBYTE, TEncode(&MqBean{Topic: topic, ID: 0, Msg: msg}))
}

func (this *Cli) PubJson(topic string, msg string) (_r int64, err error) {
	return this._sendMsg(MQ_PUBJSON, JEncode(topic, 0, msg))
}

func (this *Cli) PubMem(topic string, msg string) (_r int64, err error) {
	return this._sendMsg(MQ_PUBMEM, JEncode(topic, 0, msg))
}

func (this *Cli) Sub(topic string) (_r int64, err error) {
	return this._sendMsg(MQ_SUB, []byte(topic))
}

func (this *Cli) PullByte(topic string, id int64) (_r int64, err error) {
	return this._sendMsg(MQ_PULLBYTE, TEncode(&MqBean{Topic: topic, ID: id}))
}

func (this *Cli) PullJson(topic string, id int64) (_r int64, err error) {
	return this._sendMsg(MQ_PULLJSON, JEncode(topic, id, ""))
}

func (this *Cli) _send(bs []byte) (err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	return websocket.Message.Send(this.conn, bs)
}

func (this *Cli) _sendMsg(tlType byte, bs []byte) (_r int64, err error) {
	var buf bytes.Buffer
	buf.WriteByte(tlType)
	_r = getAckId()
	buf.Write(Int64ToBytes(_r))
	if bs != nil {
		buf.Write([]byte(bs))
	}
	err = this._send(buf.Bytes())
	return
}

func (this *Cli) Auth(str string) (_r int64, err error) {
	return this._sendMsg(MQ_AUTH, []byte(str))
}

func (this *Cli) Close() (err error) {
	this._isError = true
	err = this.conn.Close()
	return
}

func (this *Cli) _read() {
	var err error
	for !this._isError {
		var byt []byte
		if err = websocket.Message.Receive(this.conn, &byt); err != nil {
			this._isError = true
			break
		}
		if byt != nil && this.conf.OnMessage != nil {
			this.conf.OnMessage(this, byt)
		}
	}
	if this.conf.OnError != nil {
		go this.conf.OnError(this, err)
	}
	this.Close()
	if this.conf.OnClose != nil {
		go this.conf.OnClose(this)
	}
}

func JEncode(topic string, id int64, msg string) (bs []byte) {
	bs, _ = json.Marshal(&JMqBean{Topic: topic, Id: id, Msg: msg})
	return
}

func JDecode(bs []byte) (mb *JMqBean, err error) {
	err = json.Unmarshal(bs, &mb)
	return
}

func TEncode(ts thrift.TStruct) []byte {
	buf := thrift.NewTMemoryBuffer()
	tcf := thrift.NewTCompactProtocolFactory()
	tp := tcf.GetProtocol(buf)
	ts.Write(context.Background(), tp)
	return buf.Bytes()
}

func TDecode[T thrift.TStruct](bs []byte, ts T) (_r T, err error) {
	buf := thrift.NewTMemoryBuffer()
	buf.Buffer = bytes.NewBuffer(bs)
	tcf := thrift.NewTCompactProtocolFactory()
	tp := tcf.GetProtocol(buf)
	err = ts.Read(context.Background(), tp)
	return ts, err
}

func _recover() {
	if err := recover(); err != nil {
		logging.Error(err)
	}
}

type JMqBean struct {
	Id    int64
	Topic string
	Msg   string
}

func CRC32(bs []byte) uint32 {
	return crc32.ChecksumIEEE(bs)
}

func Int64ToBytes(n int64) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, n)
	return bytesBuffer.Bytes()
}

func BytesToInt64(bs []byte) (_r int64, err error) {
	bytesBuffer := bytes.NewBuffer(bs)
	err = binary.Read(bytesBuffer, binary.BigEndian, &_r)
	return
}

func getAckId() int64 {
	return int64(CRC32(Int64ToBytes(time.Now().UnixNano())))
}
