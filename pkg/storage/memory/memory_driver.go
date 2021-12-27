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

package memory

import (
	"sync"

	"github.com/201341/elasticell/pkg/storage"

	"github.com/201341/elasticell/pkg/util"
)

type opt struct {
	key      []byte
	value    []byte
	isDelete bool
}

type memoryWriteBatch struct {
	sync.Mutex

	opts []*opt
}

func newMemoryWriteBatch() storage.WriteBatch {
	return &memoryWriteBatch{}
}

func (wb *memoryWriteBatch) Delete(key []byte) error {
	wb.Lock()
	defer wb.Unlock()

	wb.opts = append(wb.opts, &opt{
		key:      key,
		isDelete: true,
	})

	return nil
}

func (wb *memoryWriteBatch) Set(key []byte, value []byte) error {
	wb.Lock()
	defer wb.Unlock()

	wb.opts = append(wb.opts, &opt{
		key:   key,
		value: value,
	})

	return nil
}

type memoryDriver struct {
	metaEngine storage.Engine
	dataEngine storage.DataEngine
	kvEngine   storage.KVEngine
	hashEngine storage.HashEngine
	listEngine storage.ListEngine
	setEngine  storage.SetEngine
	zsetEngine storage.ZSetEngine
}

// NewMemoryDriver returns Driver with memory implemention
func NewMemoryDriver() storage.Driver {
	kv := util.NewKVTree()
	return &memoryDriver{
		metaEngine: newMemoryMetaEngine(),
		kvEngine:   newMemoryKVEngine(kv),
		dataEngine: newMemoryDataEngine(kv),
	}
}

func (d *memoryDriver) GetEngine() storage.Engine {
	return d.metaEngine
}

func (d *memoryDriver) GetDataEngine() storage.DataEngine {
	return d.dataEngine
}

func (d *memoryDriver) GetKVEngine() storage.KVEngine {
	return d.kvEngine
}

func (d *memoryDriver) GetHashEngine() storage.HashEngine {
	return d.hashEngine
}

func (d *memoryDriver) GetListEngine() storage.ListEngine {
	return d.listEngine
}

func (d *memoryDriver) GetSetEngine() storage.SetEngine {
	return d.setEngine
}

func (d *memoryDriver) GetZSetEngine() storage.ZSetEngine {
	return d.zsetEngine
}

func (d *memoryDriver) NewWriteBatch() storage.WriteBatch {
	return newMemoryWriteBatch()
}

func (d *memoryDriver) Write(wb storage.WriteBatch, sync bool) error {
	mwb := wb.(*memoryWriteBatch)

	for _, opt := range mwb.opts {
		if opt.isDelete {
			d.metaEngine.Delete(opt.key)
		} else {
			d.metaEngine.Set(opt.key, opt.value)
		}
	}

	return nil
}
