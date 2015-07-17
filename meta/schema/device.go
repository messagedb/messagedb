package schema

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

type Device struct {
	Id        bson.ObjectId `json:"id" bson:"_id,omitempty"`
	UserId    bson.ObjectId `json:"user_id" bson:"user_id,omitempty"`
	CreatedAt time.Time     `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time     `json:"updated_at" bson:"updated_at"`
	Errors    Errors        `json:"-" bson:"-"`
}
