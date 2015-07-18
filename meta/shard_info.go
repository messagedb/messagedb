package meta

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/messagedb/messagedb/meta/internal"
)

// ShardGroupInfo represents metadata about a shard group. The DeletedAt field is important
// because it makes it clear that a ShardGroup has been marked as deleted, and allow the system
// to be sure that a ShardGroup is not simply missing. If the DeletedAt is set, the system can
// safely delete any associated shards.
type ShardGroupInfo struct {
	ID        uint64
	StartTime time.Time
	EndTime   time.Time
	DeletedAt time.Time
	Shards    []ShardInfo
}

// Contains return true if the shard group contains data for the timestamp.
func (sgi *ShardGroupInfo) Contains(timestamp time.Time) bool {
	return !sgi.StartTime.After(timestamp) && sgi.EndTime.After(timestamp)
}

// Overlaps return whether the shard group contains data for the time range between min and max
func (sgi *ShardGroupInfo) Overlaps(min, max time.Time) bool {
	return !sgi.StartTime.After(max) && sgi.EndTime.After(min)
}

// Deleted returns whether this ShardGroup has been deleted.
func (sgi *ShardGroupInfo) Deleted() bool {
	return !sgi.DeletedAt.IsZero()
}

// clone returns a deep copy of sgi.
func (sgi ShardGroupInfo) clone() ShardGroupInfo {
	other := sgi

	if sgi.Shards != nil {
		other.Shards = make([]ShardInfo, len(sgi.Shards))
		for i := range sgi.Shards {
			other.Shards[i] = sgi.Shards[i].clone()
		}
	}

	return other
}

// ShardFor returns the ShardInfo for a Point hash
func (sgi *ShardGroupInfo) ShardFor(hash uint64) ShardInfo {
	return sgi.Shards[hash%uint64(len(sgi.Shards))]
}

// marshal serializes to a protobuf representation.
func (sgi *ShardGroupInfo) marshal() *internal.ShardGroupInfo {
	pb := &internal.ShardGroupInfo{
		ID:        proto.Uint64(sgi.ID),
		StartTime: proto.Int64(MarshalTime(sgi.StartTime)),
		EndTime:   proto.Int64(MarshalTime(sgi.EndTime)),
		DeletedAt: proto.Int64(MarshalTime(sgi.DeletedAt)),
	}

	pb.Shards = make([]*internal.ShardInfo, len(sgi.Shards))
	for i := range sgi.Shards {
		pb.Shards[i] = sgi.Shards[i].marshal()
	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (sgi *ShardGroupInfo) unmarshal(pb *internal.ShardGroupInfo) {
	sgi.ID = pb.GetID()
	sgi.StartTime = UnmarshalTime(pb.GetStartTime())
	sgi.EndTime = UnmarshalTime(pb.GetEndTime())
	sgi.DeletedAt = UnmarshalTime(pb.GetDeletedAt())

	sgi.Shards = make([]ShardInfo, len(pb.GetShards()))
	for i, x := range pb.GetShards() {
		sgi.Shards[i].unmarshal(x)
	}
}

// ShardInfo represents metadata about a shard.
type ShardInfo struct {
	ID       uint64
	OwnerIDs []uint64
}

// OwnedBy returns whether the shard's owner IDs includes nodeID.
func (si ShardInfo) OwnedBy(nodeID uint64) bool {
	for _, id := range si.OwnerIDs {
		if id == nodeID {
			return true
		}
	}
	return false
}

// clone returns a deep copy of si.
func (si ShardInfo) clone() ShardInfo {
	other := si

	if si.OwnerIDs != nil {
		other.OwnerIDs = make([]uint64, len(si.OwnerIDs))
		copy(other.OwnerIDs, si.OwnerIDs)
	}

	return other
}

// marshal serializes to a protobuf representation.
func (si ShardInfo) marshal() *internal.ShardInfo {
	pb := &internal.ShardInfo{
		ID: proto.Uint64(si.ID),
	}

	pb.OwnerIDs = make([]uint64, len(si.OwnerIDs))
	copy(pb.OwnerIDs, si.OwnerIDs)

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (si *ShardInfo) unmarshal(pb *internal.ShardInfo) {
	si.ID = pb.GetID()
	si.OwnerIDs = make([]uint64, len(pb.GetOwnerIDs()))
	copy(si.OwnerIDs, pb.GetOwnerIDs())
}

// ShardGroupInfos reprenset an array of ShardGroupInfo types
type ShardGroupInfos []ShardGroupInfo

func (a ShardGroupInfos) Len() int           { return len(a) }
func (a ShardGroupInfos) Less(i, j int) bool { return a[i].StartTime.Before(a[j].StartTime) }
func (a ShardGroupInfos) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
