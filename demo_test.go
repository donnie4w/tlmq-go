/**
* Copyright 2023 tldb Author. All Rights Reserved.
* email: donnie4w@gmail.com
* https://github.com/donnie4w/tldb
* https://github.com/donnie4w/tlmq-go
**/
package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/donnie4w/simplelog/logging"
	"github.com/donnie4w/tlmq-go/cli"
	. "github.com/donnie4w/tlmq-go/stub"
)

func Test_cli(t *testing.T) {
	logging.Debug("go mqcli demo run")
	sc := cli.NewMqClient("wss://127.0.0.1:5100", "mymq=123")
	sc.PubByteHandler(func(mb *MqBean) { logging.Debug("PubByte >> ", mb) })
	sc.PubJsonHandler(func(jmb *JMqBean) { logging.Debug("PubJson >> ", jmb) })
	sc.PubMemHandler(func(jmb *JMqBean) { logging.Debug("PubMem >> ", jmb) })
	sc.PullByteHandler(func(mb *MqBean) { logging.Debug("PullByte >> ", mb) })
	sc.PullJsonHandler(func(jmb *JMqBean) { logging.Debug("PullJson >> ", jmb) })
	sc.AckHandler(func(id int64) { logging.Debug("ack >> ", id) })
	sc.ErrHandler(func(id int64) { logging.Error("err code >> ", id) })
	sc.Connect()

	sc.Sub("usertable")
	sc.SubJson("usertable2")
	sc.PubByte("usertable2", []byte("abcd123456789011"))
	// for k := 0; k < 10; k++ {
	// 	go func() {
	// 		for i := 0; i < 100; i++ {
	// 			sc.PubJson("usertable", fmt.Sprint("this is go PubJson >> ", i))
	// 		}
	// 	}()
	// }

	sc.PubJson("usertable2", "pub json to usertable2")
	sc.PubByte("usertable2", []byte("pub binary to usertable2"))
	// for i := 0; i < 20; i++ {
	// 	sc.PubMem("usertable3", "this is go PubMem >>"+strconv.Itoa(i))
	// }
	logging.Debug(35)
	time.Sleep(10 * time.Second)
}

func Test_pullsync(t *testing.T) {
	sc := cli.NewMqClient("ws://127.0.0.1:5100", "mymq=123")
	sc.Connect()
	for i := 0; i < 1; i++ {
		fmt.Println(sc.PullIdSync("usertable"))
	}
	time.Sleep(7 * time.Millisecond)
}

func TestLock(t *testing.T) {
	logging.SetFormat(logging.FORMAT_TIME)
	i := 0
	go lock1(&i)
	go lock2(&i)
	go lock3(&i)
	time.Sleep(15 * time.Second)
	fmt.Println("finish thread number >>", i)
}

func lock1(k *int) {
	sc := cli.NewMqClient("ws://127.0.0.1:5001", "mymq=123")
	sc.Connect()
	for i := 0; i < 5; i++ {
		go func(i int) {
			key, _ := sc.Lock("testlock2", 3)
			logging.Debug("node-1,thread-id-", i, "  running")
			if i == 3 {
				logging.Debug("node-1,thread-id-", i, "  sleep 3 second")
				time.Sleep(3 * time.Second)
			}
			defer func() {
				logging.Debug("node-1,thread-id-", i, "  unlock")
				sc.UnLock(key)
			}()
			*k++
		}(i)
	}
}

func lock2(k *int) {
	sc := cli.NewMqClient("ws://127.0.0.1:5002", "mymq=123")
	sc.Connect()
	for i := 0; i < 5; i++ {
		go func(i int) {
			key, _ := sc.Lock("testlock2", 3)
			logging.Debug("node-2,thread-id-", i, "  running")
			if i == 3 {
				logging.Debug("node-2,thread-id-", i, "  sleep 2 second")
				time.Sleep(2 * time.Second)
			}
			defer func() {
				logging.Debug("node-2,thread-id-", i, "  unlock")
				sc.UnLock(key)
			}()
			*k++
		}(i)
	}
}

func lock3(k *int) {
	sc := cli.NewMqClient("ws://127.0.0.1:5003", "mymq=123")
	sc.Connect()
	for i := 0; i < 5; i++ {
		go func(i int) {
			key, _ := sc.Lock("testlock2", 3)
			logging.Debug("node-3,thread-id-", i, "  running")
			if i == 3 {
				logging.Debug("node-3,thread-id-", i, "  sleep 2 second")
				time.Sleep(2 * time.Second)
			}
			defer func() {
				logging.Debug("node-3,thread-id-", i, "  unlock")
				sc.UnLock(key)
			}()
			*k++
		}(i)
	}
}

func TestTryLock(t *testing.T) {
	logging.SetFormat(logging.FORMAT_MICROSECNDS)
	i:=0
	go trylock()
	go lock1(&i)
	time.Sleep(100 * time.Second)
}

func trylock() {
	sc := cli.NewMqClient("ws://127.0.0.1:5001", "mymq=123")
	sc.Connect()
	for i := 0; i < 5; i++ {
		go func(i int) {
			for {
				if token, ok := sc.TryLock("testlock2", 3); ok {
					logging.Debug("trylock thread-id-",i,",success, sleep 2 second")
					time.Sleep(2 * time.Second)
					logging.Debug("trylock thread-id-",i,", unlock")
					sc.UnLock(token)
					break
				} else {
					<-time.After(105 * time.Millisecond)
				}
			}
		}(i)
	}
}
