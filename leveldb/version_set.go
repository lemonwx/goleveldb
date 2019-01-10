package leveldb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/lemonwx/goleveldb/leveldb/env"
	"github.com/lemonwx/goleveldb/leveldb/utils"
	"github.com/lemonwx/log"
)

const (
	levelNum              = 5
	kL0_CompactionTrigger = 4
)

type BySmallestKey struct {
	internal_comparator *utils.InternalKeyComparator
}

func NewBySmallestKey(f1, f2 *FileMetaData) {

}

func (bsk *BySmallestKey) compare(f1, f2 *FileMetaData) bool {
	r := bsk.internal_comparator.Compare(f1.smallest.String(), f2.smallest.String())
	if r != 0 {
		return r < 0
	} else {
		return f1.number < f2.number
	}
}

type LevelState struct {
	deleted_files map[uint64]struct{}
	added_files   map[*FileMetaData]BySmallestKey
}

type Builder struct {
	vset_   *VersionSet
	base_   *Version
	levels_ [levelNum]*LevelState
}

func NewBuilder(vs *VersionSet, base *Version) *Builder {
	b := &Builder{vset_: vs, base_: base}
	for level := 0; level < levelNum; level += 1 {
		b.levels_[level] = &LevelState{
			deleted_files: map[uint64]struct{}{},
			added_files:   map[*FileMetaData]BySmallestKey{},
		}
	}
	return b
}

func (b *Builder) Apply(ve *VersionEdit) {
	// todo: iter ve's compact pointers
	// todo: iter ve's deletes files
	// todo: iter ve's new files
}

func (b *Builder) upper_bound(files []*FileMetaData, tgt *FileMetaData, cmp *BySmallestKey) int {
	for idx, f := range files {
		if cmp.compare(f, tgt) {
			return idx
		}
	}
	return len(files)
}

func (b *Builder) SaveTo(v *Version) {
	cmp := &BySmallestKey{}
	cmp.internal_comparator = b.vset_.icmp_
	maxUpperIdx := 0
	for level := 0; level < levelNum; level += 1 {
		base_files := b.base_.files_[level]
		added_files := b.levels_[level].added_files
		for f, _ := range added_files {
			idx := b.upper_bound(base_files, f, cmp)
			if idx > maxUpperIdx {
				maxUpperIdx = idx
			}
			for i := 0; i < idx; i += 1 {
				b.MaybeAddFile(v, level, base_files[i])
			}
			b.MaybeAddFile(v, level, f)
		}
		for idx := maxUpperIdx; maxUpperIdx < len(base_files); idx += 1 {
			b.MaybeAddFile(v, level, base_files[idx])
		}
	}
}

func (b *Builder) MaybeAddFile(v *Version, level int, f *FileMetaData) {
	_, ok := b.levels_[level].deleted_files[f.number]
	if ok {
	} else {
		files := v.files_[level]
		if level > 0 && len(files) != 0 {
			f.refs += 1
			files = append(files, f)
		}
	}

}

type VersionSet struct {
	comparator_           string
	dbname_               string
	next_file_number_     uint64
	icmp_                 *utils.InternalKeyComparator
	current_              *Version
	dummy_versions_       *Version
	compaction_level_     int
	compaction_score_     float64
	manifest_file_number_ uint64
	last_sequence_        SequenceNumber
	log_number_           uint64
	prev_log_number_      uint64
	opts                  *Options
	descriptor_file_      *env.WritableFile
	descriptor_log_       *LogWriter
	compact_pointer_      [levelNum]string
}

func (vs *VersionSet) WriteSnapshot(log *LogWriter) error {
	// TODO: Break up into multiple records to reduce memory usage on recovery?

	// Save metadata
	edit := NewVersionEdit()
	edit.SetComparatorName(vs.icmp_.User_comparator().Name())
	for level := 0; level < levelNum; level += 1 {
		if len(vs.compact_pointer_[level]) != 0 {
			k := &InternalKey{}
			k.DecodeFrom(vs.compact_pointer_[level])
			edit.SetComparatorPointer(level, k)
		}
	}
	for level := 0; level < levelNum; level += 1 {
		files := vs.current_.files_[level]
		// for i := 0; i < len(f); i += 1 {
		for _, f := range files {
			edit.AddFile(level, f.number, f.file_size, f.smallest, f.largest)
		}
	}
	record := edit.Encode()
	return log.AddRecord(record)
}

func (vs *VersionSet) LogAndApply(edit *VersionEdit, mu sync.Mutex) error {
	if edit.has_log_number_ {
		// todo: assert
	} else {
		edit.SetLogNumber(vs.log_number_)
	}
	if !edit.has_prev_log_number_ {
		edit.SetPrevLogNumber(vs.prev_log_number_)
	}
	edit.SetNextFile(vs.next_file_number_)
	edit.SetLastSequence(vs.last_sequence_)

	v := NewVersion(vs)
	builder := NewBuilder(vs, vs.current_)
	builder.Apply(edit)
	builder.SaveTo(v)
	vs.Finalize(v)

	// Initialize new descriptor log file if necessary by creating
	// a temporary file that contains a snapshot of the current version.
	var new_manifest_file string
	if vs.descriptor_log_ == nil {
		// No reason to unlock *mu here since we only hit this path in the
		// first call to LogAndApply (when opening the database).
		// todo: assert(descriptor_file_ == nullptr);
		new_manifest_file = DescriptorFileName(vs.dbname_, vs.manifest_file_number_)
		edit.SetNextFile(vs.next_file_number_)
		var err error
		vs.descriptor_file_, err = env.NewWritableFile(new_manifest_file)
		if err != nil {
			return err
		}
		vs.descriptor_log_ = NewLogWriter(vs.descriptor_file_)
		err = vs.WriteSnapshot(vs.descriptor_log_)
		if err != nil {
			return err
		}
	}

	// Unlock during expensive MANIFEST log write
	{
		mu.Unlock()
		// Write new record to MANIFEST log
		record := edit.Encode()
		if err := vs.descriptor_log_.AddRecord(record); err != nil {
			log.Errorf("MANIFEST write failed: %v\n", err)
			return err
		}
		if err := vs.descriptor_file_.F.Sync(); err != nil {
			log.Errorf("MANIFEST write failed: %v\n", err)
			return err
		}

		// If we just created a new descriptor file, install it by writing a
		// new CURRENT file that points to it.
		if len(new_manifest_file) != 0 {
			SetCurrentFile(vs.dbname_, vs.manifest_file_number_)
		}
		mu.Lock()
	}

	// Install the new version
	vs.AppendVersion(v)
	vs.log_number_ = edit.log_number_
	vs.prev_log_number_ = edit.prev_log_number_
	return nil
}

func (vs *VersionSet) NewFileNumber() uint64 {
	cur := vs.next_file_number_
	vs.next_file_number_ += 1
	return cur
}

func (vs *VersionSet) AddLiveFiles() map[uint64]struct{} {
	live := map[uint64]struct{}{}
	for v := vs.dummy_versions_.next_; v != vs.dummy_versions_; v = v.next_ {
		for level := 0; level < levelNum; level += 1 {
			f := v.files_[level]
			for _, f := range f {
				live[f.number] = struct{}{}
			}
		}
	}
	return live
}

func (vs *VersionSet) AppendVersion(v *Version) {
	if vs.current_ != nil {
		vs.current_.Unref()
	}
	vs.current_ = v
	vs.current_.Unref()

	v.prev_ = vs.dummy_versions_.prev_
	v.next_ = vs.dummy_versions_
	v.prev_.next_ = v
	v.next_.prev_ = v
}

func NewVersionSet(name string, opt *Options) *VersionSet {
	vs := &VersionSet{dbname_: name, comparator_: opt.Comparator.Name(), opts: opt}
	vs.dummy_versions_ = NewVersion(vs)
	vs.AppendVersion(NewVersion(vs))
	return vs
}

func (vs *VersionSet) Recover(saveManifest bool) (bool, error) {
	current, err := env.ReadFileToString(CurrentFileName(vs.dbname_))
	if err != nil {
		return false, err
	}
	if len(current) == 0 || current[len(current)-1] != '\n' {
		return false, errors.New("CURRENT content: %s does not end with newline")
	}
	current = current[:len(current)-1]
	dscname := vs.dbname_ + "/" + current
	log.Debugf("dscname: %s", dscname)
	f, err := env.NewSequentialFile(dscname)
	if err != nil {
		return false, err
	}

	have_log_number := false
	have_prev_log_number := false
	have_next_file := false
	have_last_sequence := false
	next_file := uint64(0)
	last_sequence := SequenceNumber(0)
	log_number := uint64(0)
	prev_log_number := uint64(0)
	reader := NewLogReader(f)
	builder := NewBuilder(vs, vs.current_)
	for {
		// todo: review reader.ReadRecord
		record, err := reader.ReadRecord()
		if err != nil {
			break
		}
		edit := NewVersionEdit()
		if err := edit.DecodeFrom(record); err != nil {
			return false, err
		}
		if edit.comparator_ != vs.comparator_ {
			err := fmt.Errorf("%s does not match exising comparator %s", edit.comparator_, vs.comparator_)
			log.Error(err)
			return false, err
		}
		builder.Apply(edit) // todo: impl apply

		if edit.has_log_number_ {
			have_log_number = true
			log_number = edit.log_number_
		}
		if edit.has_prev_log_number_ {
			have_prev_log_number = true
			prev_log_number = edit.prev_log_number_
		}
		if edit.has_next_file_number_ {
			have_next_file = true
			next_file = edit.next_file_number_
		}
		if edit.has_last_sequence_ {
			have_last_sequence = true
			last_sequence = edit.last_sequence_
		}
	}
	f.F.Close()
	if !have_next_file {
		log.Errorf("unexpected: should have next file")
	}
	if !have_log_number {
		log.Errorf("unexpected: should have log number")
	}
	if !have_last_sequence {
		log.Errorf("unexpected: should have last sequence")
	}
	if !have_prev_log_number {
		prev_log_number = 0
	}
	vs.MarkFileNumberUsed(prev_log_number)
	vs.MarkFileNumberUsed(log_number)
	v := NewVersion(vs)
	builder.SaveTo(v)
	vs.Finalize(v)
	vs.AppendVersion(v)
	vs.manifest_file_number_ = next_file
	vs.next_file_number_ = next_file + 1
	vs.last_sequence_ = last_sequence
	vs.log_number_ = log_number
	vs.prev_log_number_ = prev_log_number

	if vs.ReuseManifest(dscname, current) {
	} else {
		return true, nil
	}
	return false, nil
}

func (vs *VersionSet) Encode() string {
	return "this is vs encode"
}

func (vs *VersionSet) ReuseManifest(dscname, dscbase string) bool {
	log.Debug(dscbase, dscname)
	if !vs.opts.ReuseLogs {
		return false
	}
	manifestNum, manifestType, l, err := env.ParseFileName(dscbase)
	if err != nil {
		return false
	}
	dscbase = dscbase[l:]
	if manifestType != env.KDescriptorFile {
		return false
	}
	manifestSize, err := env.GetFileSize(dscname)
	if err != nil {
		return false
	}
	if manifestSize >= vs.TargetFileSize(vs.opts) {
		return false
	}
	vs.descriptor_file_, err = env.NewAppendableFile(dscname)
	if err != nil {
		log.Errorf("Reuse MANIFEST: %s failed: %v", dscname, err)
		return false
	}
	log.Debugf("Reusing MANIFEST: %s", dscname)
	vs.descriptor_log_ = NewLogWriter(vs.descriptor_file_)
	vs.manifest_file_number_ = manifestNum
	return true
}

func (vs *VersionSet) MarkFileNumberUsed(num uint64) {
	if vs.next_file_number_ <= num {
		vs.next_file_number_ = num + 1
	}
}

func (vs *VersionSet) TargetFileSize(opt *Options) int {
	return opt.MaxFileSize
}

func (vs *VersionSet) Finalize(v *Version) {
	best_level := -1
	best_score := -1.0

	for level := 0; level < levelNum; level += 1 {
		score := float64(0)
		if level == 0 {
			score = float64(len(v.files_[level])) / float64(kL0_CompactionTrigger)
		} else {
			score = float64(vs.TotalFileSize(v.files_[level])) / vs.MaxBytesForLevel(level)
		}
		if score > best_score {
			best_level = level
			best_score = score
		}
	}
	vs.compaction_level_ = best_level
	vs.compaction_score_ = best_score
}

func (vs *VersionSet) TotalFileSize(files []*FileMetaData) int64 {
	sum := int64(0)
	for _, f := range files {
		sum += int64(f.file_size)
	}
	return sum
}

func (vs *VersionSet) MaxBytesForLevel(level int) float64 {
	result := float64(10.0 * 1048576.0)
	for level > 1 {
		result *= 10
		level -= 1
	}
	return result
}

type Version struct {
	vset_                 *VersionSet
	next_                 *Version
	prev_                 *Version
	refs_                 int
	file_to_compact_      int
	file_to_compact_level int
	compaction_score_     float64
	compaction_level_     int
	files_                [levelNum][]*FileMetaData
}

func NewVersion(vs *VersionSet) *Version {
	v := &Version{vset_: vs}
	v.next_ = v
	v.prev_ = v
	v.refs_ = 0
	v.file_to_compact_ = -1
	v.file_to_compact_level = -1
	v.compaction_score_ = -1
	v.compaction_level_ = -1
	return v
}

func (v *Version) Unref() {
	v.refs_ -= 1
	if v.refs_ == 0 {
	}
}
