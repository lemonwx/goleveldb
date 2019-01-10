package leveldb

import (
	"errors"
	"fmt"

	"github.com/lemonwx/goleveldb/leveldb/utils"
	"github.com/lemonwx/log"
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
	key   *InternalKey
}

type FileMetaData struct {
	refs          int
	allowed_seeks int // Seeks allowed until compaction
	number        uint64
	file_size     uint64       // File size in bytes
	smallest      *InternalKey // Smallest internal key served by table
	largest       *InternalKey // Largest internal key served by table

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

func (ve *VersionEdit) Clear() {
	ve.comparator_ = ve.comparator_[:0]
	ve.log_number_ = 0
	ve.prev_log_number_ = 0
	ve.last_sequence_ = 0
	ve.next_file_number_ = 0
	ve.has_comparator_ = false
	ve.has_log_number_ = false
	ve.has_prev_log_number_ = false
	ve.has_next_file_number_ = false
	ve.has_last_sequence_ = false
	ve.deleted_files_ = map[int]uint64{}
	ve.new_files_ = ve.new_files_[:0]
}

func NewVersionEdit() *VersionEdit {
	ve := &VersionEdit{}
	ve.Clear()
	return ve
}

func (ve *VersionEdit) print() {
	log.Debug("print start")
	log.Debug(ve.has_comparator_)
	log.Debug(ve.comparator_)

	log.Debug(ve.has_log_number_)
	log.Debug(ve.log_number_)

	log.Debug(ve.has_prev_log_number_)
	log.Debug(ve.prev_log_number_)

	log.Debug(ve.has_next_file_number_)
	log.Debug(ve.next_file_number_)

	log.Debug(ve.has_last_sequence_)
	log.Debug(ve.last_sequence_)

	log.Debug(len(ve.compact_pointers_))
	log.Debug(len(ve.deleted_files_))
	log.Debug(len(ve.new_files_))

	log.Debug("print end")
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

func (ve *VersionEdit) DecodeFrom(src []byte) error {
	for {
		tag, l, err := utils.GetVarInt32(src)
		if err != nil {
			if len(src) == 0 {
				return nil
			}
			return err
		}
		src = src[l:]
		log.Debug(tag, src)
		switch tag {
		case kComparator:
			ret, l, err := utils.GetLengthPrefixedString(src)
			if err != nil {
				log.Errorf("comparator name")
				return err
			} else {
				src = src[l:]
				ve.comparator_ = string(ret)
				ve.has_comparator_ = true
			}
		case kLogNumber:
			t, l, err := utils.GetVarInt64(src)
			if err != nil {
				return err
			}
			src = src[l:]
			log.Debug(t, src, l)
			ve.log_number_ = t
			ve.has_log_number_ = true
		case kNextFileNumber:
			t, l, err := utils.GetVarInt64(src)
			if err != nil {
				return err
			}
			src = src[l:]
			ve.next_file_number_ = t
			ve.has_next_file_number_ = true
		case kLastSequence:
			t, l, err := utils.GetVarInt64(src)
			if err != nil {
				return err
			}
			src = src[l:]
			ve.last_sequence_ = SequenceNumber(t)
			ve.has_last_sequence_ = true
		default:
			err := errors.New(fmt.Sprintf("unknown tag"))
			log.Error(err)
			return err
		}
	}
	return nil
}

func (ve *VersionEdit) SetPrevLogNumber(num uint64) {
	ve.prev_log_number_ = num
	ve.has_prev_log_number_ = true
}

func (ve *VersionEdit) SetComparatorPointer(level int, k *InternalKey) {
	ve.compact_pointers_ = append(ve.compact_pointers_, &compatPointer{level: uint32(level), key: k})
}

func (ve *VersionEdit) AddFile(level int, file uint64, file_sz uint64, smallest *InternalKey, largest *InternalKey) {
	f := &FileMetaData{
		number:    file,
		file_size: file_sz,
		smallest:  smallest,
		largest:   largest,
	}
	ve.new_files_ = append(ve.new_files_, &fileMeta{k: level, f: f})
}
