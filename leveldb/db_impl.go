package leveldb

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/lemonwx/goleveldb/leveldb/env"
	"github.com/lemonwx/log"
)

type DBImpl struct {
	lock            sync.Mutex
	dbName          string
	versions        *VersionSet
	opt             *Options
	logfile_        *env.WritableFile
	logfile_number_ uint64
	log_            log.Logger
	mem_            *MemTable
}

func NewDBImpl(name string, opt *Options) *DBImpl {
	dbImpl := &DBImpl{
		opt:      opt,
		dbName:   name,
		versions: NewVersionSet(name, opt),
	}
	dbImpl.SanitizeOptions() // init dbimpl.opt
	return dbImpl
}

func (db *DBImpl) SanitizeOptions() {
	if db.opt.info_log == nil {
		err := os.Mkdir(db.dbName, 0755)
		if err != nil && err != os.ErrExist {
			log.Fatal(err)
		}
		env.RenameFile(InfoLogFileName(db.dbName), OldInfoLogFileName(db.dbName))
		f, err := env.NewAppendableFile(InfoLogFileName(db.dbName))
		if err != nil {
			log.Fatal(err)
		}
		db.opt.info_log = log.NewDefaultLogger(f, log.DEBUG)
	}
	// todo: block cache
}

func (db *DBImpl) NewDB() error {
	var err error
	ve := &VersionEdit{}
	ve.SetComparatorName(db.opt.Comparator.Name())
	ve.SetLogNumber(0)
	ve.SetNextFile(2)
	ve.SetLastSequence(0)
	manifest := DescriptorFileName(db.dbName, 1)
	defer func() {
		if err != nil {
			env.DeleteFile(manifest)
		}
	}()
	f, err := env.NewWritableFile(manifest)
	if err != nil {
		return err
	}
	w := LogWriter{dest_: f}
	if err := w.AddRecord(ve.Encode()); err != nil {
		return err
	}
	if err := f.F.Close(); err != nil {
		log.Errorf("close file: %s failed: %v", manifest, err)
		return err
	}
	if err := SetCurrentFile(db.dbName, 1); err != nil {
		log.Errorf("set current file: %s failed: %v", db.dbName, err)
		return err
	}
	log.Debugf("set current file finish")
	return nil
}

func (db *DBImpl) Recover(edit *VersionEdit) (bool, error) {
	if err := env.Makedir(db.dbName, os.FileMode(0755)); err != nil {
		// Ignore error from CreateDir since the creation of the DB is
		// committed only when the descriptor is created, and this directory
		// may already exist from a previous failed creation attempt.
		log.Errorf("mkdir %s, mode: %v failed: %v", db.dbName, 0755, err)
	}
	if err := env.LockFile(LockFileName(db.dbName)); err != nil {
		return false, err
	}
	if !env.FileExists(CurrentFileName(db.dbName)) {
		if db.opt.CreateIfMissing {
			if err := db.NewDB(); err != nil {
				return false, err
			}
		}
	}
	saveManiFest, err := db.versions.Recover(false)
	if err != nil {
		return saveManiFest, err
	}
	log.Debugf("save manifest: %v", saveManiFest)
	childs, err := env.GetChildren(db.dbName)
	if err != nil {
		return saveManiFest, err
	}
	max_seq := SequenceNumber(0)
	min_log := db.versions.log_number_
	prev_log := db.versions.prev_log_number_
	expected := db.versions.AddLiveFiles()
	log.Debug(expected, min_log)
	logs := []uint64{}
	for _, child := range childs {
		num, Type, _, err := env.ParseFileName(child)
		log.Debug(child, num, Type, err)
		if err == nil {
			delete(expected, num)
			if Type == env.KLogFile && (num >= min_log || num == prev_log) {
				logs = append(logs, num)
			}
		}
	}
	if len(expected) != 0 {
		err := errors.New(fmt.Sprintf("%d missing files; e.g. %s", len(expected), db.dbName))
		log.Error(err)
		return saveManiFest, err
	}
	sort.Sort(Logs(logs))
	for i, log_num := range logs {
		last_log := (i == len(logs)-1)
		err := db.RecoverLogFile(log_num, last_log, saveManiFest, edit, max_seq)
		if err != nil {
			return saveManiFest, err
		}
		db.versions.MarkFileNumberUsed(log_num)
	}
	if db.versions.last_sequence_ < max_seq {
		db.versions.last_sequence_ = max_seq
	}
	return saveManiFest, nil
}

func (db *DBImpl) RecoverLogFile(logNum uint64, last_log bool, save_manifest bool, edit *VersionEdit, maxSeq SequenceNumber) error {
	fname := LogFileName(db.dbName, logNum)
	_, err := env.NewSequentialFile(fname)
	if err != nil {
		return err
	}

	return nil
}

type Logs []uint64

func (l Logs) Less(i, j int) bool {
	return l[i] < l[j]
}

func (l Logs) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l Logs) Len() int {
	return len(l)
}
