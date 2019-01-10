package leveldb

import (
	"github.com/lemonwx/goleveldb/leveldb/utils"
	"github.com/lemonwx/log"
)

type Options struct {
	Comparator      utils.Comparator
	CreateIfMissing bool
	ReuseLogs       bool
	MaxFileSize     int
	info_log        log.Logger
}
