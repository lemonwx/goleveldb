package utils

func FindIdx(vals []uint64, tgt uint64) int {
	for idx, v := range vals {
		if v == tgt {
			return idx
		}
	}
	return -1
}
