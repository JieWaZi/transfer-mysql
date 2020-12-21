package utils

import "encoding/binary"

func Uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
