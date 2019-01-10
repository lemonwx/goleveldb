package leveldb

import (
	"fmt"

	"github.com/lemonwx/goleveldb/leveldb/env"
)

func LockFileName(file string) string {
	return file + "/LOCK"
}

func CurrentFileName(file string) string {
	return file + "/CURRENT"
}

func DescriptorFileName(file string, number uint64) string {
	return fmt.Sprintf("%s/MANIFEST-%06d", file, number)
}

func MakeFileName(dbname string, num uint64, suffix string) string {
	return fmt.Sprintf("%s/%06d.%s", dbname, num, suffix)
}

func TempFileName(name string, descNum uint64) string {
	return MakeFileName(name, descNum, "dbtmp")
}

func SetCurrentFile(dbname string, descNum uint64) error {
	// Remove leading "dbname/" and add newline to manifest file name
	manifest := DescriptorFileName(dbname, descNum)
	manifest = manifest[len(dbname)+1:]
	tmp := TempFileName(dbname, descNum)
	if err := env.WriteStringToFileSync(manifest+"\n", tmp); err != nil {
		return err
	}

	if err := env.RenameFile(tmp, CurrentFileName(dbname)); err != nil {
		env.DeleteFile(tmp)
	}
	return nil
}

func InfoLogFileName(dbname string) string {
	return dbname + "/LOG"
}

// Return the name of the old info log file for "dbname".
func OldInfoLogFileName(dbname string) string {
	return dbname + "/LOG.old"
}

func LogFileName(dbname string, logNum uint64) string {
	return MakeFileName(dbname, logNum, "log")
}
