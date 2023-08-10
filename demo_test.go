/**
* Copyright 2023 tldb Author. All Rights Reserved.
* email: donnie4w@gmail.com
* https://github.com/donnie4w/tldb
* https://github.com/donnie4w/tlmq-go
**/
package main

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/donnie4w/simplelog/logging"
	"github.com/donnie4w/tlmq-go/cli"
	. "github.com/donnie4w/tlmq-go/stub"
)

func Test_cli(t *testing.T) {
	logging.Debug("go mqcli demo run")
	sc := cli.NewMqClient("ws://127.0.0.1:5000", "mymq=123")
	sc.PubByteHandler(func(mb *MqBean) { logging.Debug("PubByte >> ", mb) })
	sc.PubJsonHandler(func(jmb *JMqBean) { logging.Debug("PubJson >> ", jmb) })
	sc.PubMemHandler(func(jmb *JMqBean) { logging.Debug("PubMem >> ", jmb) })
	sc.PullByteHandler(func(mb *MqBean) { logging.Debug("PullByte >> ", mb) })
	sc.PullJsonHandler(func(jmb *JMqBean) { logging.Debug("PullJson >> ", jmb) })
	sc.AckHandler(func(id int64) { logging.Debug("ack >> ", id) })
	sc.ErrHandler(func(code int64) { logging.Error("err code >> ", code) })
	sc.Connect()

	sc.Sub("usertable")
	sc.Sub("usertable2")
	sc.PubByte("usertable", []byte("abcd1234567890"))
	for k := 0; k < 10; k++ {
		go func() {
			for i := 0; i < 100; i++ {
				sc.PubJson("usertable", fmt.Sprint("this is go PubJson >> ", i))
			}
		}()
	}

	sc.PubJson("BytesToInt64BytesToInt64BytesToInt64", "[DEBUG]2023/05/18 19:52:22 demo_test.go:43")
	for i := 0; i < 200; i++ {
		sc.PubMem("usertable3", "this is go PubMem >>"+strconv.Itoa(i))
	}
	time.Sleep(200 * time.Second)
}
