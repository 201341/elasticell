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
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/201341/elasticell/pkg/pb/mraft"
	"github.com/201341/elasticell/pkg/storage"
	"github.com/201341/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)


var (
	creating = 1
	sending  = 2
)

// SnapshotManager manager snapshot
type SnapshotManager interface {
	Register(msg *mraft.SnapshotMessage, step int) bool
	Deregister(msg *mraft.SnapshotMessage, step int)
	Create(msg *mraft.SnapshotMessage) error
	Exists(msg *mraft.SnapshotMessage) bool
	WriteTo(msg *mraft.SnapshotMessage, conn goetty.IOSession) (uint64, error)
	CleanSnap(msg *mraft.SnapshotMessage) error
	ReceiveSnapData(msg *mraft.SnapshotMessage) error
	Apply(msg *mraft.SnapshotMessage) error
}

type defaultSnapshotManager struct {
	sync.RWMutex

	limiter *rate.Limiter

	cfg *Cfg
	db  func(uint64) storage.DataEngine
	s   *Store
	dir string

	registry map[string]struct{}
}

func newDefaultSnapshotManager(cfg *Cfg, db func(uint64) storage.DataEngine, s *Store) SnapshotManager {
	dir := cfg.getSnapDir()

	if !exist(dir) {
		if err := os.Mkdir(dir, 0750); err != nil {
			log.Fatalf("raftstore-snap: cannot create dir for snapshot, errors:\n %+v",
				err)
		}
	}

	go func() {
		interval := time.Hour * 2

		for {
			log.Infof("raftstore-snap: start scan gc snap files")

			var paths []string

			err := filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
				if f == nil {
					return nil
				}

				if f.IsDir() && f.Name() == getSnapDirName() {
					return nil
				}

				var skip error
				if f.IsDir() && f.Name() != getSnapDirName() {
					skip = filepath.SkipDir
				}

				now := time.Now()
				if now.Sub(f.ModTime()) > interval {
					paths = append(paths, path)
				}

				return skip
			})

			if err != nil {
				log.Errorf("raftstore-snap: scan snap file failed, errors:\n%+v",
					err)
			}

			for _, path := range paths {
				err := os.RemoveAll(path)
				if err != nil {
					log.Errorf("raftstore-snap: scan snap file failed, file=<%s>, errors:\n%+v",
						path,
						err)
				}
			}

			time.Sleep(interval)
		}
	}()

	return &defaultSnapshotManager{
		cfg:      cfg,
		limiter:  rate.NewLimiter(rate.Every(time.Second/time.Duration(cfg.LimitSnapChunkRate)), int(cfg.LimitSnapChunkRate)),
		dir:      dir,
		db:       db,
		s:        s,
		registry: make(map[string]struct{}),
	}
}

func formatKey(msg *mraft.SnapshotMessage) string {
	return fmt.Sprintf("%d_%d_%d", msg.Header.Cell.ID, msg.Header.Term, msg.Header.Index)
}

func formatKeyStep(msg *mraft.SnapshotMessage, step int) string {
	return fmt.Sprintf("%s_%d", formatKey(msg), step)
}

func (m *defaultSnapshotManager) getPathOfSnapKey(msg *mraft.SnapshotMessage) string {
	return fmt.Sprintf("%s/%s", m.dir, formatKey(msg))
}

func (m *defaultSnapshotManager) getPathOfSnapKeyGZ(msg *mraft.SnapshotMessage) string {
	return fmt.Sprintf("%s.gz", m.getPathOfSnapKey(msg))
}

func (m *defaultSnapshotManager) getTmpPathOfSnapKeyGZ(msg *mraft.SnapshotMessage) string {
	return fmt.Sprintf("%s.tmp", m.getPathOfSnapKey(msg))
}

func (m *defaultSnapshotManager) Register(msg *mraft.SnapshotMessage, step int) bool {
	m.Lock()
	defer m.Unlock()

	fkey := formatKeyStep(msg, step)

	if _, ok := m.registry[fkey]; ok {
		return false
	}

	m.registry[fkey] = emptyStruct
	return true
}

func (m *defaultSnapshotManager) Deregister(msg *mraft.SnapshotMessage, step int) {
	m.Lock()
	defer m.Unlock()

	fkey := formatKeyStep(msg, step)
	delete(m.registry, fkey)
}

func (m *defaultSnapshotManager) inRegistry(msg *mraft.SnapshotMessage, step int) bool {
	m.RLock()
	defer m.RUnlock()

	fkey := formatKeyStep(msg, step)
	_, ok := m.registry[fkey]

	return ok
}

func (m *defaultSnapshotManager) Create(msg *mraft.SnapshotMessage) error {
	path := m.getPathOfSnapKey(msg)
	gzPath := m.getPathOfSnapKeyGZ(msg)
	start := encStartKey(&msg.Header.Cell)
	end := encEndKey(&msg.Header.Cell)
	db := m.db(msg.Header.Cell.ID)

	if !exist(gzPath) {
		if !exist(path) {
			err := db.CreateSnapshot(path, start, end)
			if err != nil {
				return errors.Wrapf(err, "")
			}
		}
		err := util.GZIP(path)
		if err != nil {
			return errors.Wrapf(err, "")
		}
	}

	info, err := os.Stat(fmt.Sprintf("%s.gz", path))
	if err != nil {
		return errors.Wrapf(err, "")
	}

	fileSize := uint64(info.Size())
	snapshotSizeHistogram.Observe(float64(fileSize))
	return nil
}

func (m *defaultSnapshotManager) Exists(msg *mraft.SnapshotMessage) bool {
	file := m.getPathOfSnapKeyGZ(msg)
	return exist(file)
}

func (m *defaultSnapshotManager) WriteTo(msg *mraft.SnapshotMessage, conn goetty.IOSession) (uint64, error) {
	file := m.getPathOfSnapKeyGZ(msg)

	if !m.Exists(msg) {
		return 0, fmt.Errorf("missing snapshot file: %s", file)
	}

	info, err := os.Stat(file)
	if err != nil {
		return 0, errors.Wrapf(err, "")
	}
	fileSize := info.Size()

	f, err := os.Open(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var written int64
	buf := make([]byte, m.cfg.LimitSnapChunkBytes)
	ctx := context.TODO()

	log.Infof("raftstore-snap[cell-%d]: try to send snap, header=<%s>,size=<%d>",
		msg.Header.Cell.ID,
		msg.Header.String(),
		fileSize)

	for {
		nr, er := f.Read(buf)
		if nr > 0 {
			dst := &mraft.SnapshotMessage{}
			dst.Header = msg.Header
			dst.Chunk = &mraft.SnapshotChunkMessage{
				Data:     buf[0:nr],
				FileSize: uint64(fileSize),
				First:    0 == written,
				Last:     fileSize == written+int64(nr),
			}

			written += int64(nr)
			err := m.limiter.Wait(ctx)
			if err != nil {
				return 0, err
			}

			err = conn.WriteAndFlush(dst)
			if err != nil {
				return 0, err
			}
		}
		if er != nil {
			if er != io.EOF {
				return 0, er
			}
			break
		}
	}

	log.Infof("raftstore-snap[cell-%d]: send snap complete",
		msg.Header.Cell.ID)
	return uint64(written), nil
}

func (m *defaultSnapshotManager) CleanSnap(msg *mraft.SnapshotMessage) error {
	var err error

	tmpFile := m.getTmpPathOfSnapKeyGZ(msg)
	if exist(tmpFile) {
		log.Infof("raftstore-snap[cell-%d]: delete exists snap tmp file, file=<%s>, header=<%s>",
			msg.Header.Cell.ID,
			tmpFile,
			msg.Header.String())
		err = os.RemoveAll(tmpFile)
	}

	if err != nil {
		return err
	}

	file := m.getPathOfSnapKeyGZ(msg)
	if exist(file) {
		log.Infof("raftstore-snap[cell-%d]: delete exists snap gz file, file=<%s>, header=<%s>",
			msg.Header.Cell.ID,
			file,
			msg.Header.String())
		err = os.RemoveAll(file)
	}

	if err != nil {
		return err
	}

	dir := m.getPathOfSnapKey(msg)
	if exist(dir) {
		log.Infof("raftstore-snap[cell-%d]: delete exists snap dir, file=<%s>, header=<%s>",
			msg.Header.Cell.ID,
			dir,
			msg.Header.String())
		err = os.RemoveAll(dir)
	}

	return err
}

func (m *defaultSnapshotManager) ReceiveSnapData(msg *mraft.SnapshotMessage) error {
	var err error
	var f *os.File

	if msg.Chunk.First {
		err = m.cleanTmp(msg)
	}

	if err != nil {
		return err
	}

	file := m.getTmpPathOfSnapKeyGZ(msg)
	if exist(file) {
		f, err = os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			f.Close()
			return err
		}
	} else {
		f, err = os.Create(file)
		if err != nil {
			f.Close()
			return err
		}
	}

	n, err := f.Write(msg.Chunk.Data)
	if err != nil {
		f.Close()
		return err
	}

	if n != len(msg.Chunk.Data) {
		f.Close()
		return fmt.Errorf("write snapshot file failed, expect=<%d> actual=<%d>",
			len(msg.Chunk.Data),
			n)
	}

	f.Close()

	if msg.Chunk.Last {
		return m.check(msg)
	}

	return nil
}

func (m *defaultSnapshotManager) Apply(msg *mraft.SnapshotMessage) error {
	file := m.getPathOfSnapKeyGZ(msg)
	if !m.Exists(msg) {
		return fmt.Errorf("missing snapshot file, path=%s", file)
	}

	defer m.CleanSnap(msg)

	err := util.UnGZIP(file, m.dir)
	if err != nil {
		return err
	}
	dir := m.getPathOfSnapKey(msg)
	defer os.RemoveAll(dir)

	// apply snapshot of data
	return m.db(msg.Header.Cell.ID).ApplySnapshot(dir)
}

func (m *defaultSnapshotManager) cleanTmp(msg *mraft.SnapshotMessage) error {
	var err error
	tmpFile := m.getTmpPathOfSnapKeyGZ(msg)
	if exist(tmpFile) {
		log.Infof("raftstore-snap[cell-%d]: delete exists snap tmp file, file=<%s>, header=<%s>",
			msg.Header.Cell.ID,
			tmpFile,
			msg.Header.String())
		err = os.RemoveAll(tmpFile)
	}

	if err != nil {
		return err
	}

	return nil
}

func (m *defaultSnapshotManager) check(msg *mraft.SnapshotMessage) error {
	file := m.getTmpPathOfSnapKeyGZ(msg)
	if exist(file) {
		info, err := os.Stat(file)
		if err != nil {
			return errors.Wrapf(err, "")
		}

		if msg.Chunk.FileSize != uint64(info.Size()) {
			return fmt.Errorf("snap file size not match, got=<%d> expect=<%d> path=<%s>",
				info.Size(),
				msg.Chunk.FileSize,
				file)
		}

		return os.Rename(file, m.getPathOfSnapKeyGZ(msg))
	}

	return fmt.Errorf("missing snapshot file, path=%s", file)
}

func exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}
