package schema

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2/bson"
)

// OwnerType represents the type of namespace owner
type OwnerType int

// Owner types
const (
	OwnerTypeUser OwnerType = iota
	OwnerTypeOrganization
)

var ownerTypes = [...]string{"user", "org"}

func (t OwnerType) String() string {
	return ownerTypes[t]
}

// MarshalJSON turns a OwnerType into a json.Marshaller.
func (t OwnerType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, ownerTypes[t])), nil
}

// Namespace represents the slug path for a User or Organization
type Namespace struct {
	Errors Errors `bson:"-"`

	ID        bson.ObjectId `bson:"_id"`
	Path      string        `bson:"path"`
	OwnerID   bson.ObjectId `bson:"owner_id,omitempty"`
	OwnerType OwnerType     `bson:"owner_type"`
	CreatedAt time.Time     `bson:"created_at"`
	UpdatedAt time.Time     `bson:"updated_at"`
}

// Slug returns the URL friendly path for the organization
func (n *Namespace) Slug() string {
	return n.Path
}

// BelongsToUser returns true if the naemspace owner is a user
func (n *Namespace) BelongsToUser() bool {
	return n.OwnerType == OwnerTypeUser
}

// BelongsToOrganization returns true if the naemspace owner is an organization
func (n *Namespace) BelongsToOrganization() bool {
	return n.OwnerType == OwnerTypeOrganization
}
