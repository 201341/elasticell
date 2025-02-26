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

package server

import (
	"github.com/201341/elasticell/pkg/pb/raftcmdpb"
	"github.com/201341/elasticell/pkg/pool"
	"github.com/201341/elasticell/pkg/redis"
)

func (s *RedisServer) onLockable(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	l := len(args)
	if l < 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onLock(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	l := len(args)
	if l < 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onUnlock(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	l := len(args)
	if l < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}
