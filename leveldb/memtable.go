package leveldb

type MemTable struct {
	refs int
}

func NewMemTable() *MemTable {
	return &MemTable{}
}

func (m *MemTable) Ref() {
	m.refs += 1
}
