package leveldb

import "github.com/lemonwx/goleveldb/leveldb/utils"

type Options struct {
	Comparator      utils.Comparator
	CreateIfMissing bool
}
