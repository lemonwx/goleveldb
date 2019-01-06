package env

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
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
