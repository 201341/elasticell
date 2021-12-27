package raftstore

import (
	"github.com/201341/elasticell/pkg/pb/mraft"
	"github.com/201341/elasticell/pkg/storage"
	"github.com/201341/elasticell/pkg/storage/memory"
	. "github.com/pingcap/check"
)

type storeCheckTestSuite struct {
}

func (s *storeCheckTestSuite) SetUpSuite(c *C) {

}

func (s *storeCheckTestSuite) TearDownSuite(c *C) {

}

func (s *storeTestSuite) TestIsRaftMsgValid(c *C) {
	store := &Store{
		id: 1,
	}

	msg := new(mraft.RaftMessage)
	msg.ToPeer.StoreID = 1
	c.Assert(store.isRaftMsgValid(msg), IsTrue)

	msg.ToPeer.StoreID = 2
	c.Assert(store.isRaftMsgValid(msg), IsFalse)
}

func (s *storeTestSuite) TestIsMsgStale(c *C) {
	store := new(Store)
	store.engines = []storage.Driver{memory.NewMemoryDriver()}
	store.enginesMask = uint64(len(store.engines) - 1)
}
