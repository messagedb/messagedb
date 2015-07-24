package v1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/tsdb"
	"github.com/messagedb/messagedb/db"
)

// Format is the file format name of this engine.
const Format = "v1"

func init() {
	db.RegisterEngine(Format, NewEngine)
}

// topLevelBucketN is the number of non-series buckets in the bolt db.
const topLevelBucketN = 3

var (
	// ErrWALPartitionNotFound returns when flushing a partition that does not exist.
	ErrWALPartitionNotFound = errors.New("wal partition not found")
)

// Ensure Engine implements the interface.
var _ db.Engine = &Engine{}

// Engine represents a version 1 storage engine.
type Engine struct {
	mu sync.Mutex

	path string   // path to data file
	db   *bolt.DB // underlying database

	cache map[uint8]map[string][][]byte // values by <wal partition,conversation>

	walSize    int           // approximate size of the WAL, in bytes
	flush      chan struct{} // signals background flush
	flushTimer *time.Timer   // signals time-based flush

	// These coordinate closing and waiting for running goroutines.
	wg      sync.WaitGroup
	closing chan struct{}

	// Used for out-of-band error messages.
	logger *log.Logger

	// The maximum size and time thresholds for flushing the WAL.
	MaxWALSize             int
	WALFlushInterval       time.Duration
	WALPartitionFlushDelay time.Duration

	// The writer used by the logger.
	LogOutput io.Writer
}

// NewEngine returns a new instance of Engine.
func NewEngine(path string, opt tsdb.EngineOptions) tsdb.Engine {
	e := &Engine{
		path:  path,
		flush: make(chan struct{}, 1),

		MaxWALSize:             opt.MaxWALSize,
		WALFlushInterval:       opt.WALFlushInterval,
		WALPartitionFlushDelay: opt.WALPartitionFlushDelay,

		LogOutput: os.Stderr,
	}

	// Initialize all partitions of the cache.
	e.cache = make(map[uint8]map[string][][]byte)
	for i := uint8(0); i < WALPartitionN; i++ {
		e.cache[i] = make(map[string][][]byte)
	}

	return e
}

// Open opens and initializes the engine.
func (e *Engine) Open() error {
	if err := func() error {
		e.mu.Lock()
		defer e.mu.Unlock()

		// Open underlying storage.
		db, err := bolt.Open(e.path, 0666, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			return err
		}
		e.db = db

		// Initialize data file.
		if err := e.db.Update(func(tx *bolt.Tx) error {
			_, _ = tx.CreateBucketIfNotExists([]byte("series"))
			_, _ = tx.CreateBucketIfNotExists([]byte("fields"))
			_, _ = tx.CreateBucketIfNotExists([]byte("wal"))

			// Set file format, if not set yet.
			b, _ := tx.CreateBucketIfNotExists([]byte("meta"))
			if v := b.Get([]byte("format")); v == nil {
				if err := b.Put([]byte("format"), []byte(Format)); err != nil {
					return fmt.Errorf("set format: %s", err)
				}
			}

			return nil
		}); err != nil {
			return fmt.Errorf("init: %s", err)
		}

		// Start flush interval timer.
		e.flushTimer = time.NewTimer(e.WALFlushInterval)

		// Initialize logger.
		e.logger = log.New(e.LogOutput, "[v1] ", log.LstdFlags)

		// Start background goroutines.
		e.wg.Add(1)
		e.closing = make(chan struct{})
		go e.autoflusher(e.closing)

		return nil
	}(); err != nil {
		e.close()
		return err
	}

	// Flush on-disk WAL before we return to the caller.
	if err := e.Flush(0); err != nil {
		return fmt.Errorf("flush: %s", err)
	}

	return nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	err := e.close()
	e.mu.Unlock()

	// Wait for open goroutines to finish.
	e.wg.Wait()
	return err
}

func (e *Engine) close() error {
	if e.db != nil {
		e.db.Close()
	}
	if e.closing != nil {
		close(e.closing)
		e.closing = nil
	}
	return nil
}

// SetLogOutput sets the writer used for log output.
// This must be set before opening the engine.
func (e *Engine) SetLogOutput(w io.Writer) { e.LogOutput = w }

// LoadMetadataIndex loads the shard metadata into memory.
func (e *Engine) LoadMetadataIndex(index *tsdb.DatabaseIndex, measurementFields map[string]*tsdb.MeasurementFields) error {
	return e.db.View(func(tx *bolt.Tx) error {

		// load series metadata
		meta = tx.Bucket([]byte("conversations"))
		c = meta.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			conversation := &db.Conversation{}
			if err := conversation.UnmarshalBinary(v); err != nil {
				return err
			}
			index.CreateConverationIndexIfNotExists(conversation)
		}
		return nil
	})
}

// WriteMessages will write the raw data points and any new metadata to the index in the shard
func (e *Engine) WriteMessages(messages []db.Message, conversation *db.Conversation) error {
	// save to the underlying bolt instance
	if err := e.db.Update(func(tx *bolt.Tx) error {

		// Write points to WAL bucket.
		wal := tx.Bucket([]byte("wal"))
		for _, m := range messages {
			// Retrieve partition bucket.
			key := m.Key()
			b, err := wal.CreateBucketIfNotExists([]byte{WALPartition(key)})
			if err != nil {
				return fmt.Errorf("create WAL partition bucket: %s", err)
			}

			// Generate an autoincrementing index for the WAL partition.
			id, _ := b.NextSequence()

			// Append points sequentially to the WAL bucket.
			v := marshalWALEntry(key, m.UnixNano(), m.Data())
			if err := b.Put(u64tob(id), v); err != nil {
				return fmt.Errorf("put wal: %s", err)
			}
		}

	}); err != nil {
		return err
	}

	// If successful then save points to in-memory cache.
	if err := func() error {
		e.mu.Lock()
		defer e.mu.Unlock()

		// tracks which in-memory caches need to be resorted
		resorts := map[uint8]map[string]struct{}{}

		for _, m := range messages {
			// Generate in-memory cache entry of <timestamp,data>.
			key, data := m.Key(), m.Data()
			v := make([]byte, 8+len(data))
			binary.BigEndian.PutUint64(v[0:8], uint64(m.UnixNano()))
			copy(v[8:], data)

			// Determine if we are appending.
			partitionID := WALPartition(key)
			a := e.cache[partitionID][string(key)]
			appending := (len(a) == 0 || bytes.Compare(a[len(a)-1], v) == -1)

			// Append to cache list.
			a = append(a, v)

			// If not appending, keep track of cache lists that need to be resorted.
			if !appending {
				convo := resorts[partitionID]
				if convo == nil {
					convo = map[string]struct{}{}
					resorts[partitionID] = convo
				}
				convo[string(key)] = struct{}{}
			}

			e.cache[partitionID][string(key)] = a

			// Calculate estimated WAL size.
			e.walSize += len(key) + len(v)
		}

	}(); err != nil {
		return err
	}

	return nil
}
