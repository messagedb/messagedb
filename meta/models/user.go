package models

//
// import (
// 	"github.com/messagedb/messagedb/meta/schema"
// 	"github.com/messagedb/messagedb/meta/utils"
//
// 	"gopkg.in/mgo.v2/bson"
// )
// //
// // var User *UserModel
// //
// // type UserModel struct {
// // 	*storage.Model
// // }
// //
// // func (m *UserModel) New() *schema.User {
// // 	return storage.CreateDocument(&schema.User{}).(*schema.User)
// // }
// //
// // func (m *UserModel) FindById(id interface{}) (*schema.User, error) {
// // 	user := &schema.User{}
// // 	err := m.FindId(utils.ObjectId(id)).One(user)
// // 	if err != nil {
// // 		return nil, err
// // 	}
// // 	return user, nil
// // }
// //
// // func (m *UserModel) FindByUsername(username string) (*schema.User, error) {
// // 	user := &schema.User{}
// // 	err := m.Find(bson.M{"username": username}).One(user)
// // 	if err != nil {
// // 		return nil, err
// // 	}
// // 	return user, nil
// // }
// //
// // func (m *UserModel) FindByEmail(email string) (*schema.User, error) {
// // 	user := &schema.User{}
// // 	err := m.Find(bson.M{"primary_email": email}).One(user)
// // 	if err != nil {
// // 		return nil, err
// // 	}
// // 	return user, nil
// // }

// Initialization routines of the Model
// func init() {
// 	User = storage.RegisterModel(schema.User{}, "users", func(col *mgo.Collection) interface{} {
// 		return &UserModel{storage.NewModel(col)}
// 	}).(*UserModel)
//
// 	// create required indexes in MongoDB
// 	indexes := []mgo.Index{}
//
// 	indexes = append(indexes, mgo.Index{
// 		Key:        []string{"username"},
// 		Unique:     true,
// 		DropDups:   true,
// 		Background: true,
// 		Sparse:     false,
// 	})
//
// 	indexes = append(indexes, mgo.Index{
// 		Key:        []string{"namespace.id"},
// 		Unique:     true,
// 		DropDups:   true,
// 		Background: true,
// 		Sparse:     false,
// 	})
//
// 	for _, index := range indexes {
// 		err := User.EnsureIndex(index)
// 		if err != nil {
// 			log.Panicf("Failed to ensure index on 'users' Collection: %v", index)
// 		}
// 	}
//
// }
