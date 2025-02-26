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

package redis

import (
	"strconv"

	"github.com/201341/elasticell/pkg/pb/raftcmdpb"
	"github.com/fagongzi/goetty"
	gredis "github.com/fagongzi/goetty/protocol/redis"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

const (
	pong = "PONG"
)

var (
	ErrNotSupportCommand  = []byte("command is not support")
	ErrInvalidCommandResp = []byte("invalid command")
	PongResp              = []byte("PONG")
	OKStatusResp          = []byte("OK")
)

// WriteFVPairArray write field value pair array resp
func WriteFVPairArray(lst []*raftcmdpb.FVPair, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if len(lst) == 0 {
		buf.Write(gredis.NullArray)
		buf.Write(gredis.Delims)
	} else {
		buf.Write(hack.StringToSlice(strconv.Itoa(len(lst) * 2)))
		buf.Write(gredis.Delims)

		for i := 0; i < len(lst); i++ {
			gredis.WriteBulk(lst[i].Field, buf)
			gredis.WriteBulk(lst[i].Value, buf)
		}
	}
}

// WriteScorePairArray write score member pair array resp
func WriteScorePairArray(lst []*raftcmdpb.ScorePair, withScores bool, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if len(lst) == 0 {
		buf.Write(gredis.NullArray)
		buf.Write(gredis.Delims)
	} else {
		if withScores {
			buf.Write(hack.StringToSlice(strconv.Itoa(len(lst) * 2)))
			buf.Write(gredis.Delims)
		} else {
			buf.Write(hack.StringToSlice(strconv.Itoa(len(lst))))
			buf.Write(gredis.Delims)
		}

		for i := 0; i < len(lst); i++ {
			gredis.WriteBulk(lst[i].Member, buf)

			if withScores {
				gredis.WriteBulk(format.Float64ToString(lst[i].Score), buf)
			}
		}
	}
}
