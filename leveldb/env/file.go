package env

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"syscall"

	"github.com/lemonwx/log"
)

func Makedir(name string, perm os.FileMode) error {
	return os.Mkdir(name, perm)
}

func LockFile(file string) error {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Debugf("open file: %s failed: %v", file, err)
		return err
	}
	pid, err := GetFileLockPid(f.Fd())
	if err != nil {
		return err
	}
	if pid != 0 {
		log.Debugf("file: %s fd: %v now locked by pid: %d, waiting...", file, f.Fd(), pid)
	}
	if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLKW, &syscall.Flock_t{Start: 0, Len: 0, Type: syscall.F_WRLCK, Whence: io.SeekStart}); err != nil {
		log.Errorf("lock file: %s fd: %v failed: %v", file, f.Fd(), err)
		return err
	}
	log.Debugf("lock file: %s fd: %v success", file, f.Fd())
	return nil
}

func FileExists(name string) bool {
	return false
}

func GetFileLockPid(fd uintptr) (int32, error) {
	t := &syscall.Flock_t{Start: 0, Len: 0, Type: syscall.F_WRLCK, Whence: io.SeekStart}
	if err := syscall.FcntlFlock(fd, syscall.F_GETLK, t); err != nil {
		log.Error("get flock of fd: %v failed: %v", fd, err)
		return 0, err
	}
	return t.Pid, nil
}

func ReadFileToString(name string) (string, error) {
	ret, err := ioutil.ReadFile(name)
	if err != nil {
		log.Errorf("read file: %s failed: %v", name, err)
		return "", err
	}
	return string(ret), nil
}

func WriteStringToFileSync(data string, name string) error {
	return DoWriteStringToFile(data, name, true)
}

func DoWriteStringToFile(data string, fname string, true bool) error {
	var err error
	defer func() {
		if err != nil {
			DeleteFile(fname)
		}
	}()
	var f *WritableFile
	f, err = NewWritableFile(fname)
	if err != nil {
		return err
	}
	if _, err = io.WriteString(f.F, data); err != nil {
		log.Errorf("write string :%s to file: %s failed: %v", data, f.F.Name(), err)
		return err
	}
	//todo: do sync based on opt
	if err = f.F.Sync(); err != nil {
		log.Errorf("sync file: %s failed: %v", f.F.Name(), err)
		return err
	}
	return nil
}

func DeleteFile(name string) error {
	if err := os.Remove(name); err != nil {
		log.Errorf("remove file: %s failed: %v", name, err)
	}
	return nil
}

func RenameFile(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		log.Errorf("rename from: %s to: %s failed: %v", from, to, err)
		return err
	}
	return nil
}

type WritableFile struct {
	F *os.File
}

func NewWritableFile(name string) (*WritableFile, error) {
	f, err := os.OpenFile(name, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("open file: %s failed: %v", name, err)
		return nil, err
	}
	return &WritableFile{F: f}, nil
}

func (wf *WritableFile) Write(b []byte) (int, error) {
	return wf.F.Write(b)
}

type SequentialFile struct {
	F *os.File
}

func NewSequentialFile(name string) (*SequentialFile, error) {
	f, err := os.OpenFile(name, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("open file: %s failed: %v", name, err)
		return nil, err
	}
	return &SequentialFile{F: f}, nil
}

func (f *SequentialFile) Read(size int, scratch []byte) ([]byte, error) {
	if len(scratch) != size {
		err := errors.New("unexpected read size not equal with lens of buffer")
		log.Error(err)
		return nil, err
	}
	n, err := f.F.Read(scratch)
	if err != nil {
		log.Errorf("read file: %s failed: %v", f.F.Name(), err)
		return nil, err
	}
	result := make([]byte, n)
	copy(result, scratch)
	return result, nil
}

type FileType int

const (
	KLogFile FileType = iota
	KDBLockFile
	KTableFile
	KDescriptorFile
	KCurrentFile
	KTempFile
	KInfoLogFile // Either the current one, or an old one
)

func ParseFileName(filename string) (uint64, FileType, int, error) {
	preLen := len(filename)
	number := uint64(0)
	var Type FileType
	switch filename {
	case "CURRENT":
		number = 0
		Type = KCurrentFile
	case "LOCK":
		number = 0
		Type = KDBLockFile
	case "LOG", "LOG.old":
		number = 0
		Type = KInfoLogFile
	default:
		if strings.HasPrefix(filename, "MANIFEST-") {
			filename = strings.TrimLeft(filename, "MANIFEST-")
			num, l, err := ConsumeDecimalNumber([]byte(filename))
			if err != nil {
				return 0, 0, 0, err
			}
			filename = filename[l:]
			if len(filename) != 0 {
				return 0, 0, 0, errors.New("unexpected")
			}
			Type = KDescriptorFile
			number = num
		} else {
			num, l, err := ConsumeDecimalNumber([]byte(filename))
			if err != nil {
				return 0, 0, 0, err
			}
			filename = filename[l:]
			if filename == ".log" {
				Type = KLogFile
			} else if filename == ".sst" || filename == ".ldb" {
				Type = KTableFile
			} else if filename == ".dbtmp" {
				Type = KTempFile
			} else {
				return 0, 0, 0, errors.New("unexpected")
			}
			number = num
		}
	}
	return number, Type, preLen - len(filename), nil
}

func GetFileSize(filename string) (int, error) {
	return 0, nil
}
func NewAppendableFile(filename string) (*WritableFile, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &WritableFile{F: f}, nil
}

func ConsumeDecimalNumber(in []byte) (uint64, int, error) {
	kMaxUint64 := uint64(1<<64 - 1)
	kLastDigitOfMaxUint64 := byte('0' + kMaxUint64%10)
	value := uint64(0)
	idx := 0
	for ; idx < len(in); idx += 1 {
		v := in[idx]
		if v < '0' || v > '9' {
			break
		}
		if value > kMaxUint64/10 || value == kMaxUint64/10 && v > kLastDigitOfMaxUint64 {
			return 0, 0, errors.New("unexpectd")
		}
		value = (value * 10) + uint64(v-'0')
	}
	digist_consumed := idx
	if digist_consumed == 0 {
		return 0, 0, errors.New("unexpectd")
	}
	return value, digist_consumed, nil
}

func GetChildren(dirName string) ([]string, error) {
	infos, err := ioutil.ReadDir(dirName)
	if err != nil {
		return nil, err
	}
	rets := make([]string, 0, len(infos))
	for _, info := range infos {
		rets = append(rets, info.Name())
	}
	return rets, nil
}
