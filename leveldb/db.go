package leveldb

import (
	"github.com/lemonwx/goleveldb/leveldb/env"
	"github.com/lemonwx/log"
)

type DB struct {
}

func Open(name string, opt *Options) (*DBImpl, error) {
	dbimpl := NewDBImpl(name, opt)
	dbimpl.lock.Lock()
	defer dbimpl.lock.Unlock()
	edit := NewVersionEdit()
	saveManifest, err := dbimpl.Recover(edit)
	if err != nil {
		return nil, err
	}
	// todo: chk dbImpl.mem_ != nil
	new_log_number := dbimpl.versions.NewFileNumber()
	logFile, err := env.NewWritableFile(LogFileName(dbimpl.dbName, new_log_number))
	if err != nil {
		return nil, err
	}
	edit.SetLogNumber(new_log_number)
	dbimpl.logfile_ = logFile
	dbimpl.logfile_number_ = new_log_number
	dbimpl.log_ = log.NewDefaultLogger(dbimpl.logfile_, log.DEBUG)
	dbimpl.mem_ = NewMemTable()
	dbimpl.mem_.Ref()
	if saveManifest {
		edit.prev_log_number_ = 0
		edit.log_number_ = dbimpl.logfile_number_
		log.Debug(dbimpl.logfile_number_)
		dbimpl.versions.LogAndApply(edit, dbimpl.lock)
	}
	dbimpl.DeleteObsoleteFiles()
	dbimpl.MaybeScheduleCompaction()
	return dbimpl, nil
}

type DDB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
}
