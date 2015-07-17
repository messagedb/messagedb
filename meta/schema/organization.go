package schema

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

// Organization represents the an organization, team, group or company in the system
type Organization struct {
	ID bson.ObjectId `bson:"_id"`

	Namespace struct {
		ID        bson.ObjectId `bson:"id"`
		Path      string        `bson:"path"`
		OwnerType OwnerType     `bson:"owner_type"`
	} `bson:"namespace"`

	Name         string    `bson:"name"`
	Description  string    `bson:"description"`
	URL          string    `bson:"url"`
	Location     string    `bson:"location"`
	Email        string    `bson:"email"`
	BillingEmail string    `bson:"billing_email"`
	CreatedAt    time.Time `bson:"created_at"`
	UpdatedAt    time.Time `bson:"updated_at"`
	Errors       Errors    `bson:"-"`
}
