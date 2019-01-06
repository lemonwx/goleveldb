package leveldb

import (
	"os"
	"sync"

	"github.com/lemonwx/goleveldb/leveldb/env"
	"github.com/lemonwx/log"
)

type DBImpl struct {
	lock     sync.Mutex
	dbName   string
	versions *VersionSet
	opt      *Options
}

func NewDBImpl(name string, opt *Options) *DBImpl {
	return &DBImpl{
		opt:      opt,
		dbName:   name,
		versions: NewVersionSet(name),
	}
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
	//io.Copy(os.Stdout, os.Stdin)
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

func (db *DBImpl) Recover() error {
	if err := env.Makedir(db.dbName, os.FileMode(0755)); err != nil {
		// Ignore error from CreateDir since the creation of the DB is
		// committed only when the descriptor is created, and this directory
		// may already exist from a previous failed creation attempt.
		log.Errorf("mkdir %s, mode: %v failed: %v", db.dbName, 0755, err)
	}
	if err := env.LockFile(LockFileName(db.dbName)); err != nil {
		return err
	}
	if !env.FileExists(CurrentFileName(db.dbName)) {
		if db.opt.CreateIfMissing {
			if err := db.NewDB(); err != nil {
				return err
			}
		}
		// todo: create if not exist base on options
	}
	if err := db.versions.Recover(false); err != nil {
		return err
	}
	return nil
}
