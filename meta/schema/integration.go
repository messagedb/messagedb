package schema

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

type Integration struct {
	Id        bson.ObjectId `json:"id" bson:"_id,omitempty"`
	Name      string        `json:"name" bson:"name"`
	CreatedAt time.Time     `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time     `json:"updated_at" bson:"updated_at"`
	Errors    Errors        `json:"-" bson:"-"`
}
