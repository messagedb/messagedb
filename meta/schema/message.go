package schema

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

type Message struct {
	Id bson.ObjectId `bson:"_id,omitempty"`

	ContentHTML      string
	ContentPlainText string

	From struct {
		UserID bson.ObjectId
		Name   string
	} `bson:"from"`

	CreatedAt time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time `json:"updated_at" bson:"updated_at"`
	Errors    Errors    `bson:"-"`
}
