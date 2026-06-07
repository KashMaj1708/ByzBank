package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketBalances  = []byte("balances")
	bucketDatastore = []byte("datastore")
	bucketLocks     = []byte("locks")
	bucketWAL       = []byte("wal")
	bucketClientTS  = []byte("clientTS")
	metaDatastoreSeq = []byte("seq") // key inside bucketDatastore
)

// Store is the concurrency-safe BoltDB-backed persistence layer for one replica.
type Store struct {
	mu             sync.Mutex
	db             *bolt.DB
	initialBalance int64
	totalItems     int
}

// Options configures a new Store.
type Options struct {
	Path           string
	InitialBalance int64
	TotalItems     int
}

// DefaultOptions returns options aligned with the 36-server topology defaults.
func DefaultOptions(path string) Options {
	topo := config.Default()
	return Options{
		Path:           path,
		InitialBalance: topo.InitialBalance,
		TotalItems:     topo.TotalItems,
	}
}

// Open creates or opens a BoltDB database at path.
func Open(opts Options) (*Store, error) {
	if opts.Path == "" {
		return nil, errors.New("store path is required")
	}
	if opts.InitialBalance == 0 {
		opts.InitialBalance = 10
	}
	if opts.TotalItems == 0 {
		opts.TotalItems = config.Default().TotalItems
	}
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0o755); err != nil && !os.IsExist(err) {
		// filepath.Dir("foo.db") is "." which exists; ignore
		if filepath.Dir(opts.Path) != "." {
			return nil, fmt.Errorf("mkdir data dir: %w", err)
		}
	}

	db, err := bolt.Open(opts.Path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}
	s := &Store{
		db:             db,
		initialBalance: opts.InitialBalance,
		totalItems:     opts.TotalItems,
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketBalances, bucketDatastore, bucketLocks, bucketWAL, bucketClientTS} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// InitialBalance returns the configured starting balance for untouched items.
func (s *Store) InitialBalance() int64 { return s.initialBalance }

// GetBalance returns the balance of item (lazy-initialised to initialBalance).
func (s *Store) GetBalance(item int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	bal, err := s.getBalance(item)
	if err != nil {
		return s.initialBalance
	}
	return bal
}

func (s *Store) getBalance(item int) (int64, error) {
	var bal int64
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		bal, err = s.balanceInTx(tx, item)
		return err
	})
	return bal, err
}

func (s *Store) balanceInTx(tx *bolt.Tx, item int) (int64, error) {
	b := tx.Bucket(bucketBalances)
	if b == nil {
		return s.initialBalance, nil
	}
	v := b.Get(itemKey(item))
	if v == nil {
		return s.initialBalance, nil
	}
	return bytesToInt64(v), nil
}

func (s *Store) setBalanceTx(tx *bolt.Tx, item int, bal int64) error {
	return tx.Bucket(bucketBalances).Put(itemKey(item), int64ToBytes(bal))
}

// ApplyTransfer atomically debits x and credits y by amt.
func (s *Store) ApplyTransfer(x, y int, amt int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		xBal, err := s.balanceInTx(tx, x)
		if err != nil {
			return err
		}
		yBal, err := s.balanceInTx(tx, y)
		if err != nil {
			return err
		}
		if xBal < amt {
			return fmt.Errorf("insufficient balance on %d: have %d need %d", x, xBal, amt)
		}
		if err := s.setBalanceTx(tx, x, xBal-amt); err != nil {
			return err
		}
		return s.setBalanceTx(tx, y, yBal+amt)
	})
}

// ApplyDebitOnly debits x by amt (coordinator prepare path).
func (s *Store) ApplyDebitOnly(x int, amt int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		bal, err := s.balanceInTx(tx, x)
		if err != nil {
			return err
		}
		if bal < amt {
			return fmt.Errorf("insufficient balance on %d: have %d need %d", x, bal, amt)
		}
		return s.setBalanceTx(tx, x, bal-amt)
	})
}

// ApplyCreditOnly credits y by amt (participant prepare path).
func (s *Store) ApplyCreditOnly(y int, amt int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		bal, err := s.balanceInTx(tx, y)
		if err != nil {
			return err
		}
		return s.setBalanceTx(tx, y, bal+amt)
	})
}

// AcquireLockForSeq acquires the lock when free, or succeeds if already held by seq.
func (s *Store) AcquireLockForSeq(item int, seq int64) bool {
	if s.IsLocked(item) {
		return s.LockSeq(item) == seq
	}
	return s.AcquireLock(item, seq)
}

// AcquireLock tries to lock item for sequence seq. Returns false if locked.
func (s *Store) AcquireLock(item int, seq int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ok := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLocks)
		key := itemKey(item)
		if b.Get(key) != nil {
			return nil
		}
		if err := b.Put(key, int64ToBytes(seq)); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return err == nil && ok
}

// ReleaseLock removes the lock on item.
func (s *Store) ReleaseLock(item int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketLocks).Delete(itemKey(item))
	})
}

// IsLocked reports whether item is currently locked.
func (s *Store) IsLocked(item int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	locked := false
	_ = s.db.View(func(tx *bolt.Tx) error {
		locked = tx.Bucket(bucketLocks).Get(itemKey(item)) != nil
		return nil
	})
	return locked
}

// LockSeq returns the owning sequence for a locked item, or 0 if unlocked.
func (s *Store) LockSeq(item int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var seq int64
	_ = s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketLocks).Get(itemKey(item))
		if v != nil {
			seq = bytesToInt64(v)
		}
		return nil
	})
	return seq
}

// AppendDatastore appends a committed entry and assigns the next sequence number.
func (s *Store) AppendDatastore(entry DatastoreEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDatastore)
		seqBytes := b.Get(metaDatastoreSeq)
		var next uint64
		if seqBytes != nil {
			next = binary.BigEndian.Uint64(seqBytes) + 1
		}
		entry.Seq = int64(next)
		body, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if err := b.Put(seqKey(next), body); err != nil {
			return err
		}
		return b.Put(metaDatastoreSeq, seqKey(next))
	})
}

// Datastore returns all committed entries in order.
func (s *Store) Datastore() ([]DatastoreEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []DatastoreEntry
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDatastore)
		return b.ForEach(func(k, v []byte) error {
			if string(k) == string(metaDatastoreSeq) {
				return nil
			}
			var e DatastoreEntry
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			out = append(out, e)
			return nil
		})
	})
	return out, err
}

// PrintDatastore returns a human-readable dump of committed entries.
func (s *Store) PrintDatastore() string {
	entries, err := s.Datastore()
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	if len(entries) == 0 {
		return "(empty datastore)"
	}
	// ForEach order is byte-sorted; seq keys are big-endian so order is correct.
	var buf string
	for i, e := range entries {
		if i > 0 {
			buf += "\n"
		}
		buf += e.String()
	}
	return buf
}

// PrintBalance returns a formatted balance line for one item on this server.
func (s *Store) PrintBalance(item int) string {
	return fmt.Sprintf("bal[%d] = %d", item, s.GetBalance(item))
}

// WALWrite stores a preimage for a pending cross-shard transaction.
func (s *Store) WALWrite(txnID string, preimage WALPreimage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, err := json.Marshal(preimage)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWAL).Put([]byte(txnID), body)
	})
}

// WALUndo restores balances from the preimage and keeps the WAL entry.
func (s *Store) WALUndo(txnID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		body := tx.Bucket(bucketWAL).Get([]byte(txnID))
		if body == nil {
			return fmt.Errorf("wal entry %q not found", txnID)
		}
		var pre WALPreimage
		if err := json.Unmarshal(body, &pre); err != nil {
			return err
		}
		for item, oldBal := range pre.Balances {
			if err := s.setBalanceTx(tx, item, oldBal); err != nil {
				return err
			}
		}
		return nil
	})
}

// WALDelete removes a WAL entry after commit.
func (s *Store) WALDelete(txnID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketWAL).Delete([]byte(txnID))
	})
}

// WALExists reports whether a WAL entry is present.
func (s *Store) WALExists(txnID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	exists := false
	_ = s.db.View(func(tx *bolt.Tx) error {
		exists = tx.Bucket(bucketWAL).Get([]byte(txnID)) != nil
		return nil
	})
	return exists
}

// SetClientTS records the last executed timestamp for a client.
func (s *Store) SetClientTS(clientID string, ts int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketClientTS).Put([]byte(clientID), int64ToBytes(ts))
	})
}

// GetClientTS returns the last executed timestamp for a client (0 if unseen).
// ClearLocksAndWAL removes in-flight lock and WAL buckets. Safe at a settled set
// boundary; committed balances and datastore are untouched.
func (s *Store) ClearLocksAndWAL() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketLocks, bucketWAL} {
			if err := tx.DeleteBucket(name); err != nil && err != bolt.ErrBucketNotFound {
				return err
			}
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) GetClientTS(clientID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ts int64
	_ = s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketClientTS).Get([]byte(clientID))
		if v != nil {
			ts = bytesToInt64(v)
		}
		return nil
	})
	return ts
}

func itemKey(item int) []byte {
	return []byte(strconv.Itoa(item))
}

func seqKey(seq uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], seq)
	return buf[:]
}

func int64ToBytes(v int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return buf[:]
}

func bytesToInt64(b []byte) int64 {
	if len(b) == 8 {
		return int64(binary.BigEndian.Uint64(b))
	}
	// legacy / unexpected
	return int64(binary.BigEndian.Uint64(append(make([]byte, 8-len(b)), b...)))
}
