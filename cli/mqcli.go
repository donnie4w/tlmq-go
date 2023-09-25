/**
* Copyright 2023 tldb Author. All Rights Reserved.
* email: donnie4w@gmail.com
* https://github.com/donnie4w/tldb
* https://github.com/donnie4w/tlmq-go
**/

package cli

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/donnie4w/simplelog/logging"
	. "github.com/donnie4w/tlmq-go/stub"
	"golang.org/x/net/websocket"
)

const (
	MQ_AUTH      byte = 1
	MQ_PUBBYTE   byte = 2
	MQ_PUBJSON   byte = 3
	MQ_SUB       byte = 4
	MQ_PULLBYTE  byte = 5
	MQ_PULLJSON  byte = 6
	MQ_PING      byte = 7
	MQ_ERROR     byte = 8
	MQ_PUBMEM    byte = 9
	MQ_RECVACK   byte = 10
	MQ_MERGE     byte = 11
	MQ_SUBCANCEL byte = 12
	MQ_CURRENTID byte = 13
	MQ_ZLIB      byte = 14
	MQ_LOCK      byte = 15
	MQ_ACK       byte = 0
)

type Config struct {
	Url       string
	Origin    string
	Auth      string
	HttpUrl   string
	RecvAckOn bool
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
	_isAuth  bool
}

func NewCli(conf *Config) (cli *Cli, err error) {
	var conn *websocket.Conn
	if conf.Origin == "" {
		conf.Origin = "http://tldb-mq"
	}
	parse(conf)
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
		cli = &Cli{conf, conn, &sync.Mutex{}, false, false}
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

func (this *Cli) SubJson(topic string) (_r int64, err error) {
	return this._sendMsg(MQ_SUB|0x80, []byte(topic))
}

func (this *Cli) Sub(topic string) (_r int64, err error) {
	return this._sendMsg(MQ_SUB, []byte(topic))
}

func (this *Cli) SubCancel(topic string) (_r int64, err error) {
	return this._sendMsg(MQ_SUBCANCEL, []byte(topic))
}

func (this *Cli) PullByte(topic string, id int64) (_r int64, err error) {
	return this._sendMsg(MQ_PULLBYTE, TEncode(&MqBean{Topic: topic, ID: id}))
}

func (this *Cli) PullJson(topic string, id int64) (_r int64, err error) {
	return this._sendMsg(MQ_PULLJSON, JEncode(topic, id, ""))
}

func (this *Cli) PullByteSync(topic string, id int64) (mb *MqBean, err error) {
	bs := append([]byte{MQ_PULLBYTE}, TEncode(&MqBean{Topic: topic, ID: id})...)
	var msg []byte
	if msg, err = httpPost(bs, this.conf, true); err == nil {
		switch msg[0] {
		case MQ_ERROR:
			err = errors.New(fmt.Sprint(bytesToInt64(msg[1:])))
		case MQ_PULLBYTE:
			mb, err = TDecode(msg[1:], &MqBean{})
		}
	}
	return
}

func (this *Cli) PullJsonSync(topic string, id int64) (jmb *JMqBean, err error) {
	bs := append([]byte{MQ_PULLJSON}, JEncode(topic, id, "")...)
	var msg []byte
	if msg, err = httpPost(bs, this.conf, true); err == nil {
		switch msg[0] {
		case MQ_ERROR:
			err = errors.New(fmt.Sprint(bytesToInt64(msg[1:])))
		case MQ_PULLJSON:
			jmb, err = JDecode(msg[1:])
		}
	}
	return
}

func (this *Cli) PullIdSync(topic string) (id int64, err error) {
	bs := append([]byte{MQ_CURRENTID}, []byte(topic)...)
	var msg []byte
	if msg, err = httpPost(bs, this.conf, true); err == nil {
		switch msg[0] {
		case MQ_ERROR:
			err = errors.New(fmt.Sprint(bytesToInt64(msg[1:])))
		case MQ_CURRENTID:
			id, err = bytesToInt64(msg[1:9])
		}
	}
	return
}

var lockMap = &sync.Map{}

func int64ToBytes(n int64) (bs []byte) {
	bs = make([]byte, 8)
	for i := 0; i < 8; i++ {
		bs[i] = byte(n >> (8 * (7 - i)))
	}
	return
}

func int32ToBytes(n int32) (bs []byte) {
	bs = make([]byte, 4)
	for i := 0; i < 4; i++ {
		bs[i] = byte(n >> (4 * (3 - i)))
	}
	return
}

func (this *Cli) TryLock(str string, overtime int32) (token string, ok bool) {
	buf := bytes.NewBuffer([]byte{})
	buf.WriteByte(MQ_LOCK)
	buf.WriteByte(2)
	strId := getAckId()
	buf.Write(int64ToBytes(strId))
	buf.Write(int32ToBytes(overtime))
	buf.WriteString(str)
	if bs, er := httpPost(buf.Bytes(), this.conf, true); er == nil {
		if bs != nil && len(bs) == 16 {
			token, ok = string(bs), true
			lockMap.Store(token, strId)
		}
	}
	return
}

func (this *Cli) Lock(str string, overtime int32) (token string, err error) {
	buf := bytes.NewBuffer([]byte{})
	buf.WriteByte(MQ_LOCK)
	buf.WriteByte(1)
	strId := getAckId()
	buf.Write(int64ToBytes(strId))
	buf.Write(int32ToBytes(overtime))
	buf.WriteString(str)
	count := 0
	for {
		if bs, er := httpPost(buf.Bytes(), this.conf, true); er == nil {
			if len(bs) == 16 {
				token = string(bs)
				lockMap.Store(token, strId)
				break
			} else if len(bs) == 1 && bs[0] == 0 {
				err = errors.New("lock failed")
				return
			} else if count == 0 {
				buf.Reset()
				buf.WriteByte(MQ_LOCK)
				buf.WriteByte(1)
				strId = getAckId()
				buf.Write(int64ToBytes(strId))
				buf.Write(int32ToBytes(overtime))
				buf.WriteString(str)
			}
		}
		count++
	}
	return
}

func (this *Cli) UnLock(token string) {
	buf := bytes.NewBuffer([]byte{})
	buf.WriteByte(MQ_LOCK)
	buf.WriteByte(3)
	v, ok := lockMap.Load(token)
	if ok {
		buf.Write(int64ToBytes(v.(int64)))
		buf.WriteString(token)
		for {
			if bs, err := httpPost(buf.Bytes(), this.conf, true); err == nil {
				if bs[0] == 1 {
					lockMap.Delete(token)
					break
				}
			} else {
				<-time.After(1 * time.Second)
			}
		}
	}
}

func (this *Cli) _send(bs []byte) (err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	return websocket.Message.Send(this.conn, bs)
}

func (this *Cli) ackMsg(bs []byte) (err error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(MQ_ACK)
	buf.Write(int64ToBytes(int64(_CRC32(bs))))
	err = this._send(buf.Bytes())
	return
}

func (this *Cli) _sendMsg(tlType byte, bs []byte) (_r int64, err error) {
	var _bs []byte
	_bs, _r = this.getSendMsg(tlType, bs)
	err = this._send(_bs)
	return
}

func (this *Cli) getSendMsg(tlType byte, bs []byte) (_bs []byte, _r int64) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(tlType)
	_r = getAckId()
	buf.Write(int64ToBytes(_r))
	if bs != nil {
		buf.Write([]byte(bs))
	}
	_bs = buf.Bytes()
	return
}

func (this *Cli) Auth(str string, cliId int64) (_r int64, err error) {
	return this._sendMsg(MQ_AUTH, []byte(str))
}

func (this *Cli) RecvAckOn(sec int8) (_r int64, err error) {
	this.conf.RecvAckOn = true
	return this._sendMsg(MQ_RECVACK, []byte{byte(sec)})
}

func (this *Cli) MergeOn(size int8) (_r int64, err error) {
	return this._sendMsg(MQ_MERGE, []byte{byte(size)})
}

func (this *Cli) SetZlib(on bool) (_r int64, err error) {
	f := byte(0)
	if on {
		f = 1
	}
	return this._sendMsg(MQ_ZLIB, []byte{f})
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

var tconf = &thrift.TConfiguration{}

func TEncode(ts thrift.TStruct) (_r []byte) {
	buf := &thrift.TMemoryBuffer{Buffer: bytes.NewBuffer([]byte{})}
	protocol := thrift.NewTCompactProtocolConf(buf, tconf)
	ts.Write(context.Background(), protocol)
	protocol.Flush(context.Background())
	_r = buf.Bytes()
	return
}

func TDecode[T thrift.TStruct](bs []byte, ts T) (_r T, err error) {
	buf := &thrift.TMemoryBuffer{Buffer: bytes.NewBuffer(bs)}
	protocol := thrift.NewTCompactProtocolConf(buf, tconf)
	err = ts.Read(context.Background(), protocol)
	return ts, err
}

func ZlibUnCz(bs []byte) (_r []byte, err error) {
	buf := bytes.NewReader(bs)
	var obuf bytes.Buffer
	var read io.ReadCloser
	if read, err = zlib.NewReader(buf); err == nil {
		defer read.Close()
		io.Copy(&obuf, read)
		_r = obuf.Bytes()
	} else {
		_r = bs
	}
	return
}

func _recover() {
	if err := recover(); err != nil {
		logging.Error(err)
	}
}

func _CRC32(bs []byte) uint32 {
	return crc32.ChecksumIEEE(bs)
}

func _CRC64(bs []byte) uint64 {
	return crc64.Checksum(bs, crc64.MakeTable(crc64.ECMA))
}

func bytesToInt64(bs []byte) (_r int64, err error) {
	bytesBuffer := bytes.NewBuffer(bs)
	err = binary.Read(bytesBuffer, binary.BigEndian, &_r)
	return
}

var __inc int64

func inc() int64 {
	return atomic.AddInt64(&__inc, 1)
}
func newTxId() (txid int64) {
	b := make([]byte, 16)
	copy(b[0:8], int64ToBytes(inc()))
	copy(b[8:], int64ToBytes(time.Now().UnixNano()))
	txid = int64(_CRC32(b))
	txid = txid<<32 | int64(int32(inc()))
	return
}

func getAckId() int64 {
	return newTxId()
}

func parse(conf *Config) {
	ss := strings.Split(conf.Url, "//")
	s := strings.Split(ss[1], "/")
	url := "http"
	if strings.HasPrefix(ss[0], "wss:") {
		url = "https"
	}
	url = url + "://" + s[0] + "/mq2"
	conf.HttpUrl = url
}

func httpPost(bs []byte, conf *Config, close bool) (_r []byte, err error) {
	client := http.Client{}
	bodyReader := bytes.NewReader(bs)
	tr := &http.Transport{
		DisableKeepAlives: true,
	}
	if strings.HasPrefix(conf.HttpUrl, "https:") {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	client.Transport = tr
	var req *http.Request
	if req, err = http.NewRequestWithContext(context.Background(), http.MethodPost, conf.HttpUrl, bodyReader); err == nil {
		if close {
			req.Close = true
		}
		req.Header.Set("Origin", conf.Origin)
		req.AddCookie(&http.Cookie{Name: "auth", Value: conf.Auth})
		var resp *http.Response
		if resp, err = client.Do(req); err == nil {
			if close {
				defer resp.Body.Close()
			}
			var body []byte
			if body, err = io.ReadAll(resp.Body); err == nil {
				_r = body
			}
		}
	}
	return
}
