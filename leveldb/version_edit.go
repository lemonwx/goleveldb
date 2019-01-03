package leveldb

import (
	"github.com/lemonwx/goleveldb/leveldb/utils"
)

const (
	kComparator     = 1
	kLogNumber      = 2
	kNextFileNumber = 3
	kLastSequence   = 4
	kCompactPointer = 5
	kDeletedFile    = 6
	kNewFile        = 7
	// 8 was used for large value refs
	kPrevLogNumber = 9
)

type SequenceNumber uint64

type compatPointer struct {
	level uint32
	key   InternalKey
}

type FileMetaData struct {
	refs          int
	allowed_seeks int // Seeks allowed until compaction
	number        uint64
	file_size     uint64      // File size in bytes
	smallest      InternalKey // Smallest internal key served by table
	largest       InternalKey // Largest internal key served by table

}

type fileMeta struct {
	k int
	f *FileMetaData
}

type VersionEdit struct {
	comparator_           string
	log_number_           uint64
	prev_log_number_      uint64
	next_file_number_     uint64
	last_sequence_        SequenceNumber
	has_comparator_       bool
	has_log_number_       bool
	has_prev_log_number_  bool
	has_next_file_number_ bool
	has_last_sequence_    bool
	compact_pointers_     []*compatPointer
	deleted_files_        map[int]uint64

	new_files_ []*fileMeta
}

func (ve *VersionEdit) Encode() []byte {
	dst := []byte{}
	if ve.has_comparator_ {
		utils.PutVarint32(&dst, kComparator)
		utils.PutLengthPrefixedSlice(&dst, ve.comparator_)
	}
	if ve.has_log_number_ {
		utils.PutVarint32(&dst, kLogNumber)
		utils.PutVarint64(&dst, ve.log_number_)
	}
	if ve.has_prev_log_number_ {
		utils.PutVarint32(&dst, kPrevLogNumber)
		utils.PutVarint64(&dst, ve.prev_log_number_)
	}
	if ve.has_next_file_number_ {
		utils.PutVarint32(&dst, kNextFileNumber)
		utils.PutVarint64(&dst, ve.next_file_number_)
	}
	if ve.has_last_sequence_ {
		utils.PutVarint32(&dst, kLastSequence)
		utils.PutVarint64(&dst, uint64(ve.last_sequence_))
	}
	for _, p := range ve.compact_pointers_ {
		utils.PutVarint32(&dst, kCompactPointer)
		utils.PutVarint32(&dst, p.level)
		utils.PutLengthPrefixedSlice(&dst, p.key.Encode())
	}
	for k, v := range ve.deleted_files_ {
		utils.PutVarint32(&dst, kDeletedFile)
		utils.PutVarint32(&dst, uint32(k)) // level
		utils.PutVarint64(&dst, uint64(v)) // file number
	}
	for _, f := range ve.new_files_ {
		utils.PutVarint32(&dst, kNewFile)
		utils.PutVarint32(&dst, uint32(f.k))
		utils.PutVarint64(&dst, f.f.number)
		utils.PutVarint64(&dst, f.f.file_size)
		utils.PutLengthPrefixedSlice(&dst, f.f.smallest.Encode())
		utils.PutLengthPrefixedSlice(&dst, f.f.largest.Encode())
	}
	return dst
}

func (ve *VersionEdit) SetComparatorName(name string) {
	ve.has_comparator_ = true
	ve.comparator_ = name
}

func (ve *VersionEdit) SetLogNumber(num uint64) {
	ve.has_log_number_ = true
	ve.log_number_ = num
}

func (ve *VersionEdit) SetNextFile(num uint64) {
	ve.has_next_file_number_ = true
	ve.next_file_number_ = num
}

func (ve *VersionEdit) SetLastSequence(seq SequenceNumber) {
	ve.has_last_sequence_ = true
	ve.last_sequence_ = seq
}
