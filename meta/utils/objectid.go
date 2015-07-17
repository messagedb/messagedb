package utils

import (
	"errors"

	"gopkg.in/mgo.v2/bson"
)

var (
	ErrInvalidObjectId = errors.New("Invalid ObjectID format")
)

func IsObjectId(id string) bool {
	return bson.IsObjectIdHex(id)
}

//Function will take types string or bson.ObjectId represented by a type interface{} and returns
//a type bson.ObjectId. Will panic if wrong type is passed. Will also panic if the string
//is not a valid representation of an ObjectId
func ObjectId(id interface{}) bson.ObjectId {
	var idvar bson.ObjectId
	switch id.(type) {
	case string:
		idvar = bson.ObjectIdHex(id.(string))
		break
	case bson.ObjectId:
		idvar = id.(bson.ObjectId)
	default:
		panic("Only accepts types `string` and `bson.ObjectId` accepted as Id")
	}
	return idvar
}
