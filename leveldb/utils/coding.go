package utils

import (
	"errors"
)

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
	default:
		ret = append(ret, byte(v|B), byte((v>>7)|B), byte((v>>14)|B), byte((v>>21)|B), byte(v>>28))
	}
	// todo: more big numbers
	return ret
}

func PutLengthPrefixedSlice(buf *[]byte, value string) {
	PutVarint32(buf, uint32(len(value)))
	*buf = append(*buf, []byte(value)...)
}

func GetVarInt32(input []byte) (uint32, int, error) {
	ret := uint32(0)
	for idx, v := range input {
		if v&128 != 0 {
			ret |= ((uint32(v) & 127) << uint32(idx*7))
		} else { // parse to end
			ret |= (uint32(v) << uint32(idx*7))
			return ret, idx + 1, nil
		}
		if idx == 5 {
			break // varInt32 max len is 5
		}
	}
	return 0, 0, errors.New("unexpected proto of varInt32")
}

func GetLengthPrefixedString(src []byte) ([]byte, int, error) {
	size, l, err := GetVarInt32(src)
	if err != nil {
		return nil, 0, err
	}
	src = src[l:]
	if size <= uint32(len(src)) {
		return src[:size], l + int(size), nil
	}
	return nil, 0, errors.New("size not enough")
}

func GetVarInt64(input []byte) (uint64, int, error) {
	ret := uint64(0)
	idx := 0
	size := len(input)
	for shift := uint64(0); shift <= 63 && idx < size; shift += 7 {
		b := uint64(input[idx])
		if b&128 != 0 {
			ret |= ((b & 127) << shift)
		} else {
			ret |= (b << shift)
			return ret, idx + 1, nil
		}
		idx += 1
	}
	return 0, 0, errors.New("unexpected proto of varInt64")
}
