package utils

import "strings"

type Comparator interface {
	Compare(s1, s2 string) int
	Name() string
	FindShortestSeparator(start string, limit string)
	FindShortSuccessor(key string)
}

type InternalKeyComparator struct {
}

func (ikc *InternalKeyComparator) Compare(s1, s2 string) int {
	return strings.Compare(s1, s2)
}

func (ikc *InternalKeyComparator) Name() string {
	return "leveldb.BytewiseComparator"
}

func (ikc *InternalKeyComparator) FindShortestSeparator(start string, limit string) {

}

func (ikc *InternalKeyComparator) FindShortSuccessor(key string) {

}
