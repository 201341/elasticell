package proxy

import (
	"sync"

	"github.com/201341/elasticell/pkg/pb/raftcmdpb"
	credis "github.com/201341/elasticell/pkg/redis"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/task"
)

type redisSession struct {
	sync.RWMutex

	session goetty.IOSession
	resps   *task.Queue
	addr    string

	aggLock      sync.RWMutex
	aggregations map[string]*aggregationReq
}

func newSession(session goetty.IOSession) *redisSession {
	return &redisSession{
		session:      session,
		resps:        &task.Queue{},
		addr:         session.RemoteAddr(),
		aggregations: make(map[string]*aggregationReq),
	}
}

func (rs *redisSession) close() {
	rs.Lock()
	rs.resps.Dispose()
	log.Infof("redis-[%s]: closed", rs.addr)
	rs.Unlock()
}

func (rs *redisSession) addAggregation(id []byte, req *aggregationReq) {
	rs.aggLock.Lock()
	rs.aggregations[string(id)] = req
	rs.aggLock.Unlock()
}

func (rs *redisSession) resp(rsp *raftcmdpb.Response) {
	if !isAggregationPart(rsp.UUID) {
		rs.resps.Put(rsp)
		return
	}

	log.Debugf("read a aggregation part: %+v", rsp.UUID)
	id, index := parseAggregationPart(rsp.UUID)
	log.Debugf("parsed a aggregation part: %+v, %d", id, index)
	rs.aggLock.RLock()
	if req, ok := rs.aggregations[hack.SliceToString(id)]; ok {
		if req.addPart(index, rsp) {
			rs.resps.Put(req.merge())
		}
	}
	rs.aggLock.RUnlock()
}

func (rs *redisSession) errorResp(err error) {
	rs.resp(&raftcmdpb.Response{
		ErrorResult: hack.StringToSlice(err.Error()),
	})
}

func (rs *redisSession) writeLoop() {
	items := make([]interface{}, batch, batch)

	for {
		n, err := rs.resps.Get(batch, items)
		if nil != err {
			return
		}

		rs.RLock()
		if !rs.session.IsConnected() {
			rs.RUnlock()
			return
		}

		buf := rs.session.OutBuf()
		for i := int64(0); i < n; i++ {
			rs.doResp(items[i].(*raftcmdpb.Response), buf)
		}
		rs.session.Flush()
		rs.RUnlock()
	}
}

func (rs *redisSession) doResp(resp *raftcmdpb.Response, buf *goetty.ByteBuf) {
	if resp.ErrorResult != nil {
		redis.WriteError(resp.ErrorResult, buf)
	}

	if resp.ErrorResults != nil {
		for _, err := range resp.ErrorResults {
			redis.WriteError(err, buf)
		}
	}

	if len(resp.BulkResult) > 0 || resp.HasEmptyBulkResult {
		redis.WriteBulk(resp.BulkResult, buf)
	}

	if len(resp.FvPairArrayResult) > 0 || resp.HasEmptyFVPairArrayResult {
		credis.WriteFVPairArray(resp.FvPairArrayResult, buf)
	}

	if resp.IntegerResult != nil {
		redis.WriteInteger(*resp.IntegerResult, buf)
	}

	if len(resp.ScorePairArrayResult) > 0 || resp.HasEmptyScorePairArrayResult {
		credis.WriteScorePairArray(resp.ScorePairArrayResult, resp.Withscores, buf)
	}

	if len(resp.SliceArrayResult) > 0 || resp.HasEmptySliceArrayResult {
		redis.WriteSliceArray(resp.SliceArrayResult, buf)
	}

	if len(resp.StatusResult) > 0 {
		redis.WriteStatus(resp.StatusResult, buf)
	}

	log.Debugf("redis-[%s]: response normal, resp=<%+v>",
		rs.addr,
		resp)
}
