package mr

import "sync"

// MapLen returns the length of the sync.Map
func MapLen(mp *sync.Map) (length int) {
	mp.Range(func(key, value any) bool {
		length++
		return true
	})
	return
}
