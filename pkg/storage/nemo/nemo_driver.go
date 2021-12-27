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

// +build freebsd openbsd netbsd dragonfly linux

package nemo

import (
	"runtime"

	"github.com/201341/elasticell/pkg/storage"
	gonemo "github.com/deepfabric/go-nemo"
)

// NemoCfg nemo cfg
type NemoCfg struct {
	DataPath              string
	OptionPath            string
	LimitConcurrencyWrite uint64
}

type nemoDrvier struct {
	db         *gonemo.NEMO
	metaEngine storage.Engine
	dataEngine storage.DataEngine
	kvEngine   storage.KVEngine
	hashEngine storage.HashEngine
	listEngine storage.ListEngine
	setEngine  storage.SetEngine
	zsetEngine storage.ZSetEngine
}

// NewNemoDriver return a driver implemention by nemo
func NewNemoDriver(cfg *NemoCfg) (storage.Driver, error) {
	var opts *gonemo.Options

	if cfg.OptionPath != "" {
		opts, _ = gonemo.NewOptions(cfg.OptionPath)
	} else {
		opts = gonemo.NewDefaultOptions()
	}

	db := gonemo.OpenNemo(opts, cfg.DataPath)

	driver := &nemoDrvier{
		db: db,
	}

	driver.init(cfg)

	return driver, nil
}

func (n *nemoDrvier) init(cfg *NemoCfg) {
	if cfg.LimitConcurrencyWrite == 0 {
		cfg.LimitConcurrencyWrite = uint64(runtime.NumCPU())
	}

	n.metaEngine = newNemoMetaEngine(n.db, cfg)
	n.dataEngine = newNemoDataEngine(n.db, cfg)
	n.kvEngine = newNemoKVEngine(n.db, cfg)
	n.hashEngine = newNemoHashEngine(n.db, cfg)
	n.listEngine = newNemoListEngine(n.db, cfg)
	n.setEngine = newNemoSetEngine(n.db, cfg)
	n.zsetEngine = newNemoZSetEngine(n.db, cfg)
}

func (n *nemoDrvier) GetEngine() storage.Engine {
	return n.metaEngine
}

func (n *nemoDrvier) GetDataEngine() storage.DataEngine {
	return n.dataEngine
}

func (n *nemoDrvier) GetKVEngine() storage.KVEngine {
	return n.kvEngine
}

func (n *nemoDrvier) GetHashEngine() storage.HashEngine {
	return n.hashEngine
}

func (n *nemoDrvier) GetListEngine() storage.ListEngine {
	return n.listEngine
}

func (n *nemoDrvier) GetSetEngine() storage.SetEngine {
	return n.setEngine
}

func (n *nemoDrvier) GetZSetEngine() storage.ZSetEngine {
	return n.zsetEngine
}

func (n *nemoDrvier) NewWriteBatch() storage.WriteBatch {
	wb := gonemo.NewWriteBatch()
	return newNemoWriteBatch(wb)
}

func (n *nemoDrvier) Write(wb storage.WriteBatch, sync bool) error {
	nwb := wb.(*nemoWriteBatch)
	return n.db.BatchWrite(n.db.GetMetaHandle(), nwb.wb, sync)
}
