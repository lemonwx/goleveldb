package utils

func PutVarint32(buf *[]byte, v uint32) {
	*buf = append(*buf, EncodeVarint32(v)...)
}

func PutVarint64(buf *[]byte, v uint64) {
	*buf = append(*buf, EncodeVarint64(v)...)
}

func EncodeVarint64(v uint64) []byte {
	dst := []byte{}
	B := uint64(128)
	for v >= B {
		dst = append(dst, byte((v&(B-1) | B)))
		v = v >> 7
	}
	dst = append(dst, byte(v))
	return dst
}

func EncodeVarint32(v uint32) []byte {
	// Operate on characters as unsigneds
	ret := []byte{}
	B := uint32(128)
	switch {
	case v < (1 << 7):
		ret = append(ret, byte(v))
	case v >= (1<<7) && v < (1<<14):
		ret = append(ret, byte(v|B), byte(v>>7))
	case v >= (1<<14) && v < (1<<21):
		ret = append(ret, byte(v|B), byte((v>>7)|B), byte(v>>14))
	case v >= (1<<21) && v < (1<<28):
		ret = append(ret, byte(v|B), byte((v>>7)|B), byte((v>>14)|B), byte(v>>21))
	}
	// todo: more big numbers
	return ret
}

func PutLengthPrefixedSlice(buf *[]byte, value string) {
	PutVarint32(buf, uint32(len(value)))
	*buf = append(*buf, []byte(value)...)
}
