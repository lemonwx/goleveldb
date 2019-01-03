package main

import (
	"os"

	"github.com/golang/leveldb/crc"

	"github.com/golang/leveldb"
	mylevel "github.com/lemonwx/goleveldb/leveldb"
	"github.com/lemonwx/goleveldb/leveldb/utils"
	"github.com/lemonwx/log"
)

func my() {
	db, err := mylevel.Open("testdb", &mylevel.Options{CreateIfMissing: true, Comparator: &utils.InternalKeyComparator{}})
	log.Debug(db, err)
}

func golangleveldb() {
	db, err := leveldb.Open("testdb", nil)
	log.Debug(db, err)
	db.Close()
}

func main() {
	os.RemoveAll("testdb")
	//golangleveldb()
	my()

	x := []byte{1, 1, 26, 108, 101, 118, 101, 108, 100, 98, 46, 66, 121, 116, 101, 119, 105, 115, 101, 67, 111, 109, 112, 97, 114, 97, 116, 111, 114, 2, 0, 3, 2, 4, 0}
	log.Debug(crc.New(x), crc.New(x).Value())

	xx := -977699691
	xxx := 160244252081149
	log.Debug(uint32(xx))
	log.Debug(uint32(xxx))
}
