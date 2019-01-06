package leveldb

import (
	"encoding/binary"

	"github.com/golang/leveldb/crc"
	"github.com/lemonwx/goleveldb/leveldb/env"
	"github.com/lemonwx/log"
)

const (
	kBlockSize  = 32768
	kHeaderSize = 4 + 2 + 1
)

const (
	kZeroType = 0
	kFullType = 1
	// For fragments
	kFirstType     = 2
	kMiddleType    = 3
	kLastType      = 4
	kMaxRecordType = kLastType
	kEof           = kMaxRecordType + 1
	kBadRecord     = kMaxRecordType + 2
)

type LogWriter struct {
	block_offset_ int
	dest_         *env.WritableFile
	type_crc      []uint32
}

func (w *LogWriter) AddRecord(record []byte) error {
	left := len(record)
	begin := true
	for left > 0 {
		leftover := kBlockSize - w.block_offset_
		if leftover < kHeaderSize {
			if leftover > 0 {
				buf := []byte{0, 0, 0, 0, 0, 0}
				if _, err := w.dest_.F.Write(buf); err != nil {
					log.Errorf("write to %s failed: %v", w.dest_.F.Name(), err)
				}
			}
			w.block_offset_ = 0
		}
		avail := kBlockSize - w.block_offset_ - kHeaderSize
		fragment_length := avail
		if left < avail {
			fragment_length = left
		}
		Type := 0
		end := left == fragment_length
		if begin && end {
			Type = kFullType
		} else if begin {
			Type = kFirstType
		} else if end {
			Type = kLastType
		} else {
			Type = kMiddleType
		}

		if err := w.EmitPhysicalRecord(Type, record, fragment_length); err != nil {
			return err
		}
		record = record[fragment_length:]
		left -= fragment_length
		begin = false
	}
	return nil
}

func (w *LogWriter) EmitPhysicalRecord(Type int, record []byte, n int) error {
	buf := make([]byte, kHeaderSize, kHeaderSize+len(record))
	buf[4] = byte(n & 0xff)
	buf[5] = byte(n >> 8)
	buf[6] = byte(Type)
	buf = append(buf, record...)
	CRC := crc.New(buf[6:]).Value()
	binary.LittleEndian.PutUint32(buf[:4], CRC)
	w.dest_.F.Write(buf)
	w.dest_.F.Sync()
	return nil
}
