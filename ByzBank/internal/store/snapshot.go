package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

// Snapshot is a portable copy of committed replica state.
type Snapshot struct {
	Balances   map[string]int64     `json:"balances"`
	ClientTS   map[string]int64     `json:"client_ts"`
	Datastore  []DatastoreEntry     `json:"datastore"`
	DatastoreSeq uint64             `json:"datastore_seq"`
}

// ExportSnapshot copies balances, datastore, and client timestamps.
func (s *Store) ExportSnapshot() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := Snapshot{
		Balances:  make(map[string]int64),
		ClientTS:  make(map[string]int64),
		Datastore: make([]DatastoreEntry, 0),
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		bal := tx.Bucket(bucketBalances)
		if bal != nil {
			_ = bal.ForEach(func(k, v []byte) error {
				item, err := strconv.Atoi(string(k))
				if err != nil {
					return nil
				}
				out.Balances[strconv.Itoa(item)] = bytesToInt64(v)
				return nil
			})
		}
		ds := tx.Bucket(bucketDatastore)
		if ds != nil {
			v := ds.Get(metaDatastoreSeq)
			if v != nil {
				out.DatastoreSeq = binary.BigEndian.Uint64(v)
			}
			_ = ds.ForEach(func(k, v []byte) error {
				if string(k) == string(metaDatastoreSeq) {
					return nil
				}
				var e DatastoreEntry
				if err := json.Unmarshal(v, &e); err != nil {
					return nil
				}
				out.Datastore = append(out.Datastore, e)
				return nil
			})
		}
		cts := tx.Bucket(bucketClientTS)
		if cts != nil {
			_ = cts.ForEach(func(k, v []byte) error {
				out.ClientTS[string(k)] = bytesToInt64(v)
				return nil
			})
		}
		return nil
	})
	return out, err
}

// ImportSnapshot replaces local committed state (locks and WAL are cleared).
func (s *Store) ImportSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketBalances, bucketDatastore, bucketLocks, bucketWAL, bucketClientTS} {
			if err := tx.DeleteBucket(name); err != nil {
				return err
			}
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		bal := tx.Bucket(bucketBalances)
		for item, v := range snap.Balances {
			if err := bal.Put(itemKey(mustAtoi(item)), int64ToBytes(v)); err != nil {
				return err
			}
		}
		ds := tx.Bucket(bucketDatastore)
		if err := ds.Put(metaDatastoreSeq, seqKey(snap.DatastoreSeq)); err != nil {
			return err
		}
		for _, e := range snap.Datastore {
			b, err := json.Marshal(e)
			if err != nil {
				return err
			}
			if err := ds.Put(seqKey(uint64(e.Seq)), b); err != nil {
				return err
			}
		}
		cts := tx.Bucket(bucketClientTS)
		for id, ts := range snap.ClientTS {
			if err := cts.Put([]byte(id), int64ToBytes(ts)); err != nil {
				return err
			}
		}
		return nil
	})
}

func mustAtoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// SnapshotJSON marshals a snapshot for HTTP transport.
func SnapshotJSON(snap Snapshot) ([]byte, error) {
	return json.Marshal(snap)
}

// ParseSnapshotJSON decodes a snapshot from HTTP transport.
func ParseSnapshotJSON(b []byte) (Snapshot, error) {
	var snap Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return snap, fmt.Errorf("parse snapshot: %w", err)
	}
	return snap, nil
}
