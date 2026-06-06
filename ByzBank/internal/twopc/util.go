package twopc

import (
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
)

func waitItemUnlocked(st *store.Store, item int, timeout, interval time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if !st.IsLocked(item) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}
