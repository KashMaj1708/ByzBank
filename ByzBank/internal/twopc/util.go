package twopc

import (
	"context"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
)

func waitItemUnlocked(ctx context.Context, st *store.Store, item int, timeout, interval time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		if !st.IsLocked(item) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(interval):
		}
	}
}
