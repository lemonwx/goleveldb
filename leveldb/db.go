package leveldb

type DB struct {
}

func Open(name string, opt *Options) (*DB, error) {
	dbimpl := NewDBImpl(name, opt)
	dbimpl.lock.Lock()
	if err := dbimpl.Recover(); err != nil {
		return nil, err
	}
	return &DB{}, nil
}
