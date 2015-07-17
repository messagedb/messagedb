package models

// import (
// 	"github.com/messagedb/messagedb/meta/schema"
// 	"github.com/messagedb/messagedb/meta/utils"
//
// 	"gopkg.in/mgo.v2/bson"
// )
//
// // Instance
// var Namespace *NamespaceModel
//
// // NamespaceModel represents the storage for the schema.Namespace
// type NamespaceModel struct {
// 	*storage.Model
// }
//
// // New creates a new instance of the Namespace model
// func (m *NamespaceModel) New() *schema.Namespace {
// 	return storage.CreateDocument(&schema.Namespace{}).(*schema.Namespace)
// }
//
// // FindByPath returns a namespace by URL path
// func (m *NamespaceModel) FindByPath(path string) (*schema.Namespace, error) {
// 	namespace := &schema.Namespace{}
// 	err := m.Find(bson.M{"path": path}).One(namespace)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return namespace, nil
// }
//
// // FindByID returns a namespace by ID. The ID parameter can be either a ID string or an bson.ObjectID
// func (m *NamespaceModel) FindByID(id interface{}) (*schema.Namespace, error) {
// 	namespace := &schema.Namespace{}
// 	err := m.Find(bson.M{"_id": utils.ObjectId(id)}).One(namespace)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return namespace, nil
// }

// func init() {
// 	Namespace = storage.RegisterModel(schema.Namespace{}, "namespaces", func(col *mgo.Collection) interface{} {
// 		return &NamespaceModel{storage.NewModel(col)}
// 	}).(*NamespaceModel)
//
// 	// create required indexes in MongoDB
// 	indexes := []mgo.Index{}
//
// 	indexes = append(indexes, mgo.Index{
// 		Key:        []string{"path"},
// 		Unique:     true,
// 		DropDups:   true,
// 		Background: true, // See notes.
// 		Sparse:     false,
// 	})
//
// 	for _, index := range indexes {
// 		err := Namespace.EnsureIndex(index)
// 		if err != nil {
// 			log.Panicf("Failed to ensure index on 'users' Collection: %v", index)
// 		}
// 	}
//
// }
