package pebble

import (
	"github.com/201341/elasticell/pkg/storage"
	"github.com/cockroachdb/pebble"
)

type PebbleCfg struct {
	DataPath              string
	OptionPath            string
	LimitConcurrencyWrite uint64
}

type pebbleDrvier struct {
	db         *pebble.DB
	metaEngine storage.Engine
	dataEngine storage.DataEngine
	kvEngine   storage.KVEngine
	hashEngine storage.HashEngine
	listEngine storage.ListEngine
	setEngine  storage.SetEngine
	zsetEngine storage.ZSetEngine
}

func NewPebbleDriver() {
	
}






