// Copyright 2016 DeepFabric, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"github.com/201341/elasticell/pkg/pb/mraft"
	"github.com/201341/elasticell/pkg/pool"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

var (
	decoder = goetty.NewIntLengthFieldBasedDecoder(newRaftDecoder())
	encoder = newRaftEncoder()
)

const (
	typeRaft = 1
	typeSnap = 2
	typeAck  = 3
)

type raftDecoder struct {
}

type raftEncoder struct {
}

func newRaftDecoder() *raftDecoder {
	return &raftDecoder{}
}

func newRaftEncoder() *raftEncoder {
	return &raftEncoder{}
}

func (decoder raftDecoder) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	t, err := in.ReadByte()
	if err != nil {
		return true, nil, err
	}

	data := in.GetMarkedRemindData()

	switch t {
	case typeSnap:
		msg := &mraft.SnapshotMessage{}
		protoc.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	case typeRaft:
		msg := pool.AcquireRaftMessage()
		protoc.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	case typeAck:
		msg := &mraft.ACKMessage{}
		protoc.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	}

	log.Fatalf("bug: not support msg type, type=<%d>", t)
	return false, nil, nil
}

func (e raftEncoder) Encode(data interface{}, out *goetty.ByteBuf) error {
	t := typeRaft
	var m protoc.PB

	if msg, ok := data.(*mraft.RaftMessage); ok {
		t = typeRaft
		m = msg
	} else if msg, ok := data.(*mraft.SnapshotMessage); ok {
		t = typeSnap
		m = msg
	} else if msg, ok := data.(*mraft.ACKMessage); ok {
		t = typeAck
		m = msg
	} else {
		log.Fatalf("bug: unsupport msg: %+v", msg)
	}

	size := m.Size()
	out.WriteInt(size + 1)
	out.WriteByte(byte(t))

	if size > 0 {
		index := out.GetWriteIndex()
		out.Expansion(size)
		protoc.MustMarshalTo(m, out.RawBuf()[index:index+size])
		out.SetWriterIndex(index + size)
	}

	return nil
}
