package leveldb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/golang/leveldb/crc"
	"github.com/lemonwx/goleveldb/leveldb/env"
	"github.com/lemonwx/log"
)

type Reporter struct {
	err error
}

func (r *Reporter) Corruption(size int, err error) {
	if r.err == nil {
		r.err = err
	}
}

type LogReader struct {
	src                   *env.SequentialFile
	initial_offset_       uint64
	last_record_offset_   uint64
	buffer_               []byte
	backing_store_        []byte
	eof_                  bool
	end_of_buffer_offset_ uint64
	reporter_             *Reporter
	checksum_             bool
	resyncing_            bool
}

func NewLogReader(f *env.SequentialFile) *LogReader {
	return &LogReader{src: f, backing_store_: make([]byte, kBlockSize)}
}

func (lr *LogReader) ReadRecord() ([]byte, error) {
	record := []byte{}
	scratch := []byte{}
	if lr.last_record_offset_ < lr.initial_offset_ {
		if !lr.SkipToInitialBlock() {
			return record, nil // todo: return tell caller continue read or not
		}
	}
	scratch = scratch[:0]
	record = record[:0]
	in_fragmented_record := false
	prospective_record_offset := uint64(0)
	for {
		fragment, record_type := lr.ReadPhysicalRecord()
		physical_record_offset := lr.end_of_buffer_offset_ - uint64(len(lr.buffer_)) - uint64(kHeaderSize) - uint64(len(record))
		if lr.resyncing_ {
			if record_type == kMiddleType {
				continue
			} else if record_type == kLastType {
				lr.resyncing_ = false
				continue
			} else {
				lr.resyncing_ = false
			}
		}

		switch record_type {
		case kFullType:
			if in_fragmented_record {
				if len(scratch) != 0 {
					lr.ReportCorruption(len(scratch), "partial record without end(1)")
				}
			}
			prospective_record_offset = physical_record_offset
			scratch = scratch[:0]
			record = fragment
			lr.last_record_offset_ = prospective_record_offset
			return record, nil
		case kFirstType:
			if in_fragmented_record {
				if len(scratch) != 0 {
					lr.ReportCorruption(len(scratch), "partial record without end(2)")
				}
			}
			prospective_record_offset = physical_record_offset
			scratch = make([]byte, len(fragment)) // todo: chk if string.assign is like this in go
			copy(scratch, fragment)
			in_fragmented_record = true
		case kMiddleType:
			if !in_fragmented_record {
				lr.ReportCorruption(len(fragment), "missing start of fragmented record(1)")
			} else {
				scratch = append(scratch, fragment...)
			}
		case kLastType:
			if !in_fragmented_record {
				lr.ReportCorruption(len(fragment), "missing start of fragmented record(2)")
			} else {
				scratch = append(scratch, fragment...)
				record = make([]byte, len(scratch))
				copy(record, scratch)
				lr.last_record_offset_ = prospective_record_offset
				return record, nil
			}
		case kEof:
			if in_fragmented_record {
				scratch = scratch[:0]
			}
			// todo: return false
		case kBadRecord:
			if in_fragmented_record {
				lr.ReportCorruption(len(scratch), "error in middle of record")
				in_fragmented_record = false
				scratch = scratch[:0]
			}
		default:
			size := len(scratch)
			if !in_fragmented_record {
				size = 0
			}
			lr.ReportCorruption(len(fragment)+size, fmt.Sprintf("unknown record type of type %u", record_type))
			in_fragmented_record = false
			scratch = scratch[:0]
		}
	}

	return nil, nil
}

func (lr *LogReader) SkipToInitialBlock() bool {
	return true
}

func (lr *LogReader) ReadPhysicalRecord() ([]byte, int) {
	var err error
	for {
		// read the header
		if len(lr.buffer_) < kHeaderSize {
			if !lr.eof_ {
				lr.buffer_ = lr.buffer_[:0]
				lr.buffer_, err = lr.src.Read(kBlockSize, lr.backing_store_)
				lr.end_of_buffer_offset_ += uint64(len(lr.buffer_))
				if err != nil {
					lr.buffer_ = lr.buffer_
					lr.ReportDrop(kBlockSize, err)
					lr.eof_ = true
					return nil, kEof
				}
				if len(lr.buffer_) < kBlockSize {
					lr.eof_ = true
				}
				continue
			} else {
				lr.buffer_ = lr.buffer_[:0]
				return nil, kEof
			}
		}

		// parse the header
		header := lr.buffer_
		a := uint32(header[4] & 0xff)
		b := uint32(header[5] & 0xff)
		Type := header[6]
		length := a | (b << 8)
		log.Debug(length)
		if uint32(kHeaderSize)+uint32(length) > uint32(len(lr.buffer_)) {
			drop_size := len(lr.buffer_)
			lr.buffer_ = lr.buffer_[:0]
			if !lr.eof_ {
				lr.ReportCorruption(drop_size, "bad record length")
				return nil, kBadRecord
			}
			return nil, kEof
		}
		if Type == kZeroType && length == 0 {
			lr.buffer_ = lr.buffer_[:0]
			return nil, kBadRecord
		}
		// check crc
		if lr.checksum_ {
			expected_crc := crc.Unmask(binary.LittleEndian.Uint32(header))
			actual_crc := crc.New(header[6:]).Value()
			if actual_crc != expected_crc {
				drop_size := len(lr.buffer_)
				lr.buffer_ = lr.buffer_[:0]
				lr.ReportCorruption(drop_size, "checksum crc mismatch")
				return nil, kBadRecord
			}
		}
		lr.remove_prefix(uint32(kHeaderSize) + length)
		if lr.end_of_buffer_offset_-uint64(len(lr.buffer_))-uint64(kHeaderSize)-uint64(length) < lr.initial_offset_ {
			return nil, kBadRecord
		}
		result := make([]byte, length)
		copy(result, header[kHeaderSize:])
		return result, int(Type)
	}
}

func (lr *LogReader) ReportDrop(size int, err error) {
	if lr.reporter_ != nil && lr.end_of_buffer_offset_-uint64(len(lr.buffer_))-uint64(size) >= lr.initial_offset_ {
		lr.reporter_.Corruption(size, err)
	}
}

func (lr *LogReader) ReportCorruption(size int, reason string) {
	lr.ReportDrop(size, errors.New(reason))
}

func (lr *LogReader) remove_prefix(size uint32) {
	if size > uint32(len(lr.buffer_)) {
		log.Fatal("prefix: %d should less than size: %d", size, len(lr.buffer_))
	}
	lr.buffer_ = lr.buffer_[size:]
}
