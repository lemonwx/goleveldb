package leveldb

type InternalKey struct {
	rep string
}

func (ik *InternalKey) Encode() string {
	return ""
}
