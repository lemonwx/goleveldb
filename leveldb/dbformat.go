package leveldb

type InternalKey struct {
	rep string
}

func (ik *InternalKey) Encode() string {
	return ""
}

func (ik *InternalKey) String() string {
	return ik.rep
}

func (ik *InternalKey) DecodeFrom(s string) {
	ik.rep = s
}
