package leveldb

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/lemonwx/goleveldb/leveldb/env"
	"github.com/lemonwx/log"
)

type ManualCompaction struct {
	level       int
	done        bool
	begin       *InternalKey
	end         *InternalKey // null means end of key range
	tmp_storage *InternalKey // Used to keep track of compaction progress
}

type DBImpl struct {
	lock            sync.Mutex
	dbName          string
	versions        *VersionSet
	opt             *Options
	logfile_        *env.WritableFile
	logfile_number_ uint64
	log_            log.Logger
	mem_            *MemTable
	imm_            *MemTable
	shutting_down_  *unsafe.Pointer

	manual_compaction_ *ManualCompaction

	background_compaction_scheduled_ bool
	background_work_finished_signal_ *sync.Cond
	bg_error                         error
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

func (db *DBImpl) DeleteObsoleteFiles() {

	//if (!bg_error_.ok()) {
	// After a background error, we don't know whether a new version may
	// or may not have been committed, so we cannot safely garbage collect.
	//return;
	//}
	if db.bg_error != nil {
		return
	}
	lives := db.versions.AddLiveFiles()
	childs, err := env.GetChildren(db.dbName)
	if err != nil {
		// todo: handle listdir err
	}
	for _, f := range childs {
		number, Type, _, err := env.ParseFileName(f)
		if err != nil {
			// todo: handle err
			continue
		}
		keep := true
		switch Type {
		case env.KLogFile:
			keep = ((number >= db.versions.log_number_) || (number == db.versions.prev_log_number_))
		case env.KDescriptorFile:
			keep = number >= db.versions.manifest_file_number_
		case env.KTableFile:
			_, keep = lives[number]
		case env.KTempFile:
			_, keep = lives[number]
		case env.KCurrentFile, env.KDBLockFile, env.KInfoLogFile:
			keep = true
		}
		if !keep {
			if Type == env.KTableFile {
				// todo: cache
			}
			db.opt.info_log.Infof("delete type=%d %s #%d\n", Type, f, number)
			env.DeleteFile(db.dbName + "/" + f)
		}
	}
}

func (db *DBImpl) MaybeScheduleCompaction() {
	if db.background_compaction_scheduled_ {
		// Already scheduled
	} else if atomic.LoadPointer(db.shutting_down_) != nil {
		// DB is being deleted; no more background compactions
	} else if db.bg_error != nil {
		// Already got an error; no more changes
	} else if db.imm_ == nil &&
		db.manual_compaction_ == nil &&
		!db.versions.NeedsCompaction() {
		// No work to be done
	} else {
		db.background_compaction_scheduled_ = true
		env.Schedule(db.BGWork)
	}
}

func (db *DBImpl) BGWork() {
	db.BackgroundCall()
}

func (db *DBImpl) BackgroundCall() {
	db.lock.Lock()
	defer db.lock.Unlock()
	if atomic.LoadPointer(db.shutting_down_) != nil {
		// No more background work when shutting down.
	} else if db.bg_error != nil {
		// No more background work after a background error.
	} else {
		db.BackgroundCompaction()
	}

	db.background_compaction_scheduled_ = false

	// Previous compaction may have produced too many files in a level,
	// so reschedule another compaction if needed.
	db.MaybeScheduleCompaction()
	db.background_work_finished_signal_.Broadcast()
}

func (db *DBImpl) BackgroundCompaction() {
	/*
	   if (db.imm_ != nil) {
	   db.CompactMemTable();
	   return;
	   }

	   Compaction* c;
	   bool is_manual = (manual_compaction_ != nullptr);
	   InternalKey manual_end;
	   if (is_manual) {
	   ManualCompaction* m = manual_compaction_;
	   c = versions_->CompactRange(m->level, m->begin, m->end);
	   m->done = (c == nullptr);
	   if (c != nullptr) {
	   manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
	   }
	   Log(options_.info_log,
	   "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
	   m->level,
	   (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
	   (m->end ? m->end->DebugString().c_str() : "(end)"),
	   (m->done ? "(end)" : manual_end.DebugString().c_str()));
	   } else {
	   c = versions_->PickCompaction();
	   }

	   Status status;
	   if (c == nullptr) {
	   // Nothing to do
	   } else if (!is_manual && c->IsTrivialMove()) {
	   // Move file to next level
	   assert(c->num_input_files(0) == 1);
	   FileMetaData* f = c->input(0, 0);
	   c->edit()->DeleteFile(c->level(), f->number);
	   c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
	   f->smallest, f->largest);
	   status = versions_->LogAndApply(c->edit(), &mutex_);
	   if (!status.ok()) {
	   RecordBackgroundError(status);
	   }
	   VersionSet::LevelSummaryStorage tmp;
	   Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
	   static_cast<unsigned long long>(f->number),
	   c->level() + 1,
	   static_cast<unsigned long long>(f->file_size),
	   status.ToString().c_str(),
	   versions_->LevelSummary(&tmp));
	   } else {
	   CompactionState* compact = new CompactionState(c);
	   status = DoCompactionWork(compact);
	   if (!status.ok()) {
	   RecordBackgroundError(status);
	   }
	   CleanupCompaction(compact);
	   c->ReleaseInputs();
	   DeleteObsoleteFiles();
	   }
	   delete c;

	   if (status.ok()) {
	   // Done
	   } else if (shutting_down_.Acquire_Load()) {
	   // Ignore compaction errors found during shutting down
	   } else {
	   Log(options_.info_log,
	   "Compaction error: %s", status.ToString().c_str());
	   }

	   if (is_manual) {
	   ManualCompaction* m = manual_compaction_;
	   if (!status.ok()) {
	   m->done = true;
	   }
	   if (!m->done) {
	   // We only compacted part of the requested range.  Update *m
	   // to the range that is left to be compacted.
	   m->tmp_storage = manual_end;
	   m->begin = &m->tmp_storage;
	   }
	   manual_compaction_ = nullptr;
	   }*/
}

func (db *DBImpl) Put(key, value []byte) error {
	batch := &WriteBatch{}
	batch.Put(key, value)
	return db.Write(batch)
}

func (db *DBImpl) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (db *DBImpl) Write(batch *WriteBatch) error {

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
