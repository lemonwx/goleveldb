package leveldb

import "github.com/lemonwx/goleveldb/leveldb/utils"

const (
	kTypeDeletion = 0x0
	kTypeValue    = 0x1
)

type WriteBatch struct {
	rep []byte
}

func (wb *WriteBatch) Put(k, v []byte) {
	wb.rep = append(wb.rep, kTypeValue)
	utils.PutLengthPrefixedSlice(&wb.rep, string(k))
	utils.PutLengthPrefixedSlice(&wb.rep, string(v))
}
