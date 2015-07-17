package cluster_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/messagedb/messagedb/cluster"
	"github.com/messagedb/messagedb/db"
	"github.com/messagedb/messagedb/meta"
)

// Ensures the messages writer maps a single message to a single shard.
func TestMessagesWriter_MapShards_One(t *testing.T) {
	ms := MetaStore{}
	rp := NewRetentionPolicy("mym", time.Hour, 3)

	ms.NodeIDFn = func() uint64 { return 1 }
	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		return &rp.ShardGroups[0], nil
	}

	c := cluster.MessagesWriter{MetaStore: ms}
	pr := &cluster.WriteMessagesRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: cluster.ConsistencyLevelOne,
	}
	pr.AddMessage("cpu", 1.0, time.Now(), nil)

	var (
		shardMappings *cluster.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if exp := 1; len(shardMappings.Messages) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings.Messages), exp)
	}
}

// Ensures the messages writer maps a multiple messages across shard group boundaries.
func TestMessagesWriter_MapShards_Multiple(t *testing.T) {
	ms := MetaStore{}
	rp := NewRetentionPolicy("mym", time.Hour, 3)
	AttachShardGroupInfo(rp, []uint64{1, 2, 3})
	AttachShardGroupInfo(rp, []uint64{1, 2, 3})

	ms.NodeIDFn = func() uint64 { return 1 }
	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		for i, sg := range rp.ShardGroups {
			if timestamp.Equal(sg.StartTime) || timestamp.After(sg.StartTime) && timestamp.Before(sg.EndTime) {
				return &rp.ShardGroups[i], nil
			}
		}
		panic("should not get here")
	}

	c := cluster.MessagesWriter{MetaStore: ms}
	pr := &cluster.WriteMessagesRequest{
		Database:         "mydb",
		RetentionPolicy:  "myrp",
		ConsistencyLevel: cluster.ConsistencyLevelOne,
	}

	// Three messages that range over the shardGroup duration (1h) and should map to two
	// distinct shards
	pr.AddMessage("cpu", 1.0, time.Unix(0, 0), nil)
	pr.AddMessage("cpu", 2.0, time.Unix(0, 0).Add(time.Hour), nil)
	pr.AddMessage("cpu", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

	var (
		shardMappings *cluster.ShardMapping
		err           error
	)
	if shardMappings, err = c.MapShards(pr); err != nil {
		t.Fatalf("unexpected an error: %v", err)
	}

	if exp := 2; len(shardMappings.Messages) != exp {
		t.Errorf("MapShards() len mismatch. got %v, exp %v", len(shardMappings.Messages), exp)
	}

	for _, messages := range shardMappings.Messages {
		// First shard shoud have 1 message w/ first message added
		if len(messages) == 1 && messages[0].Time() != pr.Messages[0].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", messages[0].Time(), pr.Messages[0].Time())
		}

		// Second shard shoud have the last two messages added
		if len(messages) == 2 && messages[0].Time() != pr.Messages[1].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", messages[0].Time(), pr.Messages[1].Time())
		}

		if len(messages) == 2 && messages[1].Time() != pr.Messages[2].Time() {
			t.Fatalf("MapShards() value mismatch. got %v, exp %v", messages[1].Time(), pr.Messages[2].Time())
		}
	}
}

func TestMessagesWriter_WriteMessages(t *testing.T) {
	tests := []struct {
		name            string
		database        string
		retentionPolicy string
		consistency     cluster.ConsistencyLevel

		// the responses returned by each shard write call.  node ID 1 = pos 0
		err    []error
		expErr error
	}{
		// Consistency one
		{
			name:            "write one success",
			database:        "mydb",
			retentionPolicy: "myrp",
			consistency:     cluster.ConsistencyLevelOne,
			err:             []error{nil, nil, nil},
			expErr:          nil,
		},
		{
			name:            "write one error",
			database:        "mydb",
			retentionPolicy: "myrp",
			consistency:     cluster.ConsistencyLevelOne,
			err:             []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:          fmt.Errorf("write failed: a failure"),
		},

		// Consistency any
		{
			name:            "write any success",
			database:        "mydb",
			retentionPolicy: "myrp",
			consistency:     cluster.ConsistencyLevelAny,
			err:             []error{fmt.Errorf("a failure"), nil, fmt.Errorf("a failure")},
			expErr:          nil,
		},
		// Consistency all
		{
			name:            "write all success",
			database:        "mydb",
			retentionPolicy: "myrp",
			consistency:     cluster.ConsistencyLevelAll,
			err:             []error{nil, nil, nil},
			expErr:          nil,
		},
		{
			name:            "write all, 2/3, partial write",
			database:        "mydb",
			retentionPolicy: "myrp",
			consistency:     cluster.ConsistencyLevelAll,
			err:             []error{nil, fmt.Errorf("a failure"), nil},
			expErr:          cluster.ErrPartialWrite,
		},
		{
			name:            "write all, 1/3 (failure)",
			database:        "mydb",
			retentionPolicy: "myrp",
			consistency:     cluster.ConsistencyLevelAll,
			err:             []error{nil, fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:          cluster.ErrPartialWrite,
		},

		// Consistency quorum
		{
			name:            "write quorum, 1/3 failure",
			consistency:     cluster.ConsistencyLevelQuorum,
			database:        "mydb",
			retentionPolicy: "myrp",
			err:             []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), nil},
			expErr:          cluster.ErrPartialWrite,
		},
		{
			name:            "write quorum, 2/3 success",
			database:        "mydb",
			retentionPolicy: "myrp",
			consistency:     cluster.ConsistencyLevelQuorum,
			err:             []error{nil, nil, fmt.Errorf("a failure")},
			expErr:          nil,
		},
		{
			name:            "write quorum, 3/3 success",
			database:        "mydb",
			retentionPolicy: "myrp",
			consistency:     cluster.ConsistencyLevelQuorum,
			err:             []error{nil, nil, nil},
			expErr:          nil,
		},

		// Error write error
		{
			name:            "no writes succeed",
			database:        "mydb",
			retentionPolicy: "myrp",
			consistency:     cluster.ConsistencyLevelOne,
			err:             []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:          fmt.Errorf("write failed: a failure"),
		},

		// Hinted handoff w/ ANY
		{
			name:            "hinted handoff write succeed",
			database:        "mydb",
			retentionPolicy: "myrp",
			consistency:     cluster.ConsistencyLevelAny,
			err:             []error{fmt.Errorf("a failure"), fmt.Errorf("a failure"), fmt.Errorf("a failure")},
			expErr:          nil,
		},

		// Write to non-existant database
		{
			name:            "write to non-existant database",
			database:        "doesnt_exist",
			retentionPolicy: "",
			consistency:     cluster.ConsistencyLevelAny,
			err:             []error{nil, nil, nil},
			expErr:          fmt.Errorf("database not found: doesnt_exist"),
		},
	}

	for _, test := range tests {

		pr := &cluster.WriteMessagesRequest{
			Database:         test.database,
			RetentionPolicy:  test.retentionPolicy,
			ConsistencyLevel: test.consistency,
		}

		// Three messages that range over the shardGroup duration (1h) and should map to two
		// distinct shards
		pr.AddMessage("cpu", 1.0, time.Unix(0, 0), nil)
		pr.AddMessage("cpu", 2.0, time.Unix(0, 0).Add(time.Hour), nil)
		pr.AddMessage("cpu", 3.0, time.Unix(0, 0).Add(time.Hour+time.Second), nil)

		// copy to prevent data race
		theTest := test
		sm := cluster.NewShardMapping()
		sm.MapMessage(
			&meta.ShardInfo{ID: uint64(1), OwnerIDs: []uint64{uint64(1), uint64(2), uint64(3)}},
			pr.Messages[0])
		sm.MapMessage(
			&meta.ShardInfo{ID: uint64(2), OwnerIDs: []uint64{uint64(1), uint64(2), uint64(3)}},
			pr.Messages[1])
		sm.MapMessage(
			&meta.ShardInfo{ID: uint64(2), OwnerIDs: []uint64{uint64(1), uint64(2), uint64(3)}},
			pr.Messages[2])

		// Local cluster.Node ShardWriter
		// lock on the write increment since these functions get called in parallel
		var mu sync.Mutex
		sw := &fakeShardWriter{
			ShardWriteFn: func(shardID, nodeID uint64, messages []db.Message) error {
				mu.Lock()
				defer mu.Unlock()
				return theTest.err[int(nodeID)-1]
			},
		}

		store := &fakeStore{
			WriteFn: func(shardID uint64, messages []db.Message) error {
				mu.Lock()
				defer mu.Unlock()
				return theTest.err[0]
			},
		}

		hh := &fakeShardWriter{
			ShardWriteFn: func(shardID, nodeID uint64, messages []db.Message) error {
				return nil
			},
		}

		ms := NewMetaStore()
		ms.DatabaseFn = func(database string) (*meta.DatabaseInfo, error) {
			return nil, nil
		}
		ms.NodeIDFn = func() uint64 { return 1 }
		c := cluster.NewMessagesWriter()
		c.MetaStore = ms
		c.ShardWriter = sw
		c.DataStore = store
		c.HintedHandoff = hh

		err := c.WriteMessages(pr)
		if err == nil && test.expErr != nil {
			t.Errorf("MessagesWriter.WriteMessages(): '%s' error: got %v, exp %v", test.name, err, test.expErr)
		}

		if err != nil && test.expErr == nil {
			t.Errorf("MessagesWriter.WriteMessages(): '%s' error: got %v, exp %v", test.name, err, test.expErr)
		}
		if err != nil && test.expErr != nil && err.Error() != test.expErr.Error() {
			t.Errorf("MessagesWriter.WriteMessages(): '%s' error: got %v, exp %v", test.name, err, test.expErr)
		}
	}
}

var shardID uint64

type fakeShardWriter struct {
	ShardWriteFn func(shardID, nodeID uint64, messages []db.Message) error
}

func (f *fakeShardWriter) WriteShard(shardID, nodeID uint64, messages []db.Message) error {
	return f.ShardWriteFn(shardID, nodeID, messages)
}

type fakeStore struct {
	WriteFn       func(shardID uint64, messages []db.Message) error
	CreateShardfn func(database, retentionPolicy string, shardID uint64) error
}

func (f *fakeStore) WriteToShard(shardID uint64, messages []db.Message) error {
	return f.WriteFn(shardID, messages)
}

func (f *fakeStore) CreateShard(database, retentionPolicy string, shardID uint64) error {
	return f.CreateShardfn(database, retentionPolicy, shardID)
}

func NewMetaStore() *MetaStore {
	ms := &MetaStore{}
	rp := NewRetentionPolicy("mym", time.Hour, 3)
	AttachShardGroupInfo(rp, []uint64{1, 2, 3})
	AttachShardGroupInfo(rp, []uint64{1, 2, 3})

	ms.RetentionPolicyFn = func(db, retentionPolicy string) (*meta.RetentionPolicyInfo, error) {
		return rp, nil
	}

	ms.CreateShardGroupIfNotExistsFn = func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
		for i, sg := range rp.ShardGroups {
			if timestamp.Equal(sg.StartTime) || timestamp.After(sg.StartTime) && timestamp.Before(sg.EndTime) {
				return &rp.ShardGroups[i], nil
			}
		}
		panic("should not get here")
	}
	return ms
}

type MetaStore struct {
	NodeIDFn                      func() uint64
	RetentionPolicyFn             func(database, name string) (*meta.RetentionPolicyInfo, error)
	CreateShardGroupIfNotExistsFn func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	DatabaseFn                    func(database string) (*meta.DatabaseInfo, error)
	ShardOwnerFn                  func(shardID uint64) (string, string, *meta.ShardGroupInfo)
}

func (m MetaStore) NodeID() uint64 { return m.NodeIDFn() }

func (m MetaStore) RetentionPolicy(database, name string) (*meta.RetentionPolicyInfo, error) {
	return m.RetentionPolicyFn(database, name)
}

func (m MetaStore) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	return m.CreateShardGroupIfNotExistsFn(database, policy, timestamp)
}

func (m MetaStore) Database(database string) (*meta.DatabaseInfo, error) {
	return m.DatabaseFn(database)
}

func (m MetaStore) ShardOwner(shardID uint64) (string, string, *meta.ShardGroupInfo) {
	return m.ShardOwnerFn(shardID)
}

func NewRetentionPolicy(name string, duration time.Duration, nodeCount int) *meta.RetentionPolicyInfo {
	shards := []meta.ShardInfo{}
	ownerIDs := []uint64{}
	for i := 1; i <= nodeCount; i++ {
		ownerIDs = append(ownerIDs, uint64(i))
	}

	// each node is fully replicated with each other
	shards = append(shards, meta.ShardInfo{
		ID:       nextShardID(),
		OwnerIDs: ownerIDs,
	})

	rp := &meta.RetentionPolicyInfo{
		Name:               "myrp",
		ReplicaN:           nodeCount,
		Duration:           duration,
		ShardGroupDuration: duration,
		ShardGroups: []meta.ShardGroupInfo{
			meta.ShardGroupInfo{
				ID:        nextShardID(),
				StartTime: time.Unix(0, 0),
				EndTime:   time.Unix(0, 0).Add(duration).Add(-1),
				Shards:    shards,
			},
		},
	}
	return rp
}

func AttachShardGroupInfo(rp *meta.RetentionPolicyInfo, ownerIDs []uint64) {
	var startTime, endTime time.Time
	if len(rp.ShardGroups) == 0 {
		startTime = time.Unix(0, 0)
	} else {
		startTime = rp.ShardGroups[len(rp.ShardGroups)-1].StartTime.Add(rp.ShardGroupDuration)
	}
	endTime = startTime.Add(rp.ShardGroupDuration).Add(-1)

	sh := meta.ShardGroupInfo{
		ID:        uint64(len(rp.ShardGroups) + 1),
		StartTime: startTime,
		EndTime:   endTime,
		Shards: []meta.ShardInfo{
			meta.ShardInfo{
				ID:       nextShardID(),
				OwnerIDs: ownerIDs,
			},
		},
	}
	rp.ShardGroups = append(rp.ShardGroups, sh)
}

func nextShardID() uint64 {
	return atomic.AddUint64(&shardID, 1)
}
