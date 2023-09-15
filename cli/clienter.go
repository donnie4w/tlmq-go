/**
* Copyright 2023 tldb Author. All Rights Reserved.
* email: donnie4w@gmail.com
* https://github.com/donnie4w/tldb
* https://github.com/donnie4w/tlmq-go
**/

package cli

import (
	. "github.com/donnie4w/tlmq-go/stub"
)

type MqClient interface {
	PullByteHandler(f func(mb *MqBean))
	PullJsonHandler(f func(jmb *JMqBean))
	PubByteHandler(f func(mb *MqBean))
	PubJsonHandler(f func(jmb *JMqBean))
	PubMemHandler(f func(jmb *JMqBean))
	AckHandler(f func(id int64))
	ErrHandler(f func(code int64))
	//method after the connection successful
	Before(f func())
	//connect to server
	Connect() (err error)
	// Subscribe to a topic
	Sub(topic string) (_r int64, err error)
	// Subscribe to a topic
	SubJson(topic string) (_r int64, err error)
	// Unsubscribed topic
	SubCancel(topic string) (_r int64, err error)

	// Publishing topic and PubByteHandler will receive it
	PubByte(topic string, msg []byte) (_r int64, err error)

	// Publishing topic and PubJsonHandler will receive it
	PubJson(topic string, msg string) (_r int64, err error)

	// the topic body is not stored when use PubMem
	PubMem(topic string, msg string) (_r int64, err error)

	// pull the topic body by topic id used asynchronization mode
	PullByte(topic string, id int64) (_r int64, err error)

	// pull the topic body by topic id used asynchronization mode
	PullJson(topic string, id int64) (_r int64, err error)

	// pull the topic body by topic id used synchronization mode
	PullByteSync(topic string, id int64) (mb *MqBean, err error)

	// pull the topic body by topic id used synchronization mode
	PullJsonSync(topic string, id int64) (jmb *JMqBean, err error)

	// pull the maximum id number of the topic
	PullIdSync(topic string) (id int64, err error)

	// setup requires a client return receipt
	RecvAckOn(sec int8) (_r int64, err error)

	// set the limit of the size of protocol data sent by the server before compression(Unit:MB)
	MergeOn(size int8) (_r int64, err error)

	SetZlib(on bool) (_r int64, err error)
}
