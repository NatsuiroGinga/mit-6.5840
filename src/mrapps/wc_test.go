package main

import (
	"strings"
	"testing"
)

func TestSplitWords(t *testing.T) {
	s := "Have A Nice Day"
	kvs := Map("test.txt", strings.ToLower(s))
	for _, kv := range kvs {
		t.Logf("key: %s, value: %s", kv.Key, kv.Value)
	}
}
