package mr

import (
	"sync"
	"unsafe"
)

// MapLen returns the length of the sync.Map
func MapLen(mp *sync.Map) (length int) {
	mp.Range(func(key, value any) bool {
		length++
		return true
	})
	return
}

type Integer interface {
	int | int64 | int32 | int16 | int8
}

func Max[T Integer](x, y T) T {
	if x > y {
		return x
	}
	return y
}

func Bytes2Str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
