package leveldb

import (
	"errors"

	"github.com/lemonwx/goleveldb/leveldb/env"
	"github.com/lemonwx/log"
)

type VersionSet struct {
	dbname string
}

func NewVersionSet(name string) *VersionSet {
	return &VersionSet{dbname: name}
}

func (vs *VersionSet) Recover(saveManifest bool) error {
	current, err := env.ReadFileToString(CurrentFileName(vs.dbname))
	if err != nil {
		return err
	}
	if len(current) == 0 || current[len(current)-1] != '\n' {
		return errors.New("CURRENT content: %s does not end with newline")
	}
	current = current[:len(current)-1]
	dscname := vs.dbname + "/" + current
	log.Debugf("dscname: %s", dscname)
	f, err := env.NewSequentialFile(dscname)
	if err != nil {
		return err
	}

	reader := NewLogReader(f)
	// todo: review reader.ReadRecord
	record, err := reader.ReadRecord()
	edit := NewVersionEdit()
	edit.de
	return nil
}

func (vs *VersionSet) Encode() string {
	return "this is vs encode"
}
