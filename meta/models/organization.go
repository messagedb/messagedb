package models

// import (
// 	"github.com/messagedb/messagedb/meta/schema"
// 	"github.com/messagedb/messagedb/meta/utils"
//
// 	"gopkg.in/mgo.v2/bson"
// )
//
// // variables
// var Organization *OrganizationModel
//
// // OrganizationModel represents an organization collection
// type OrganizationModel struct {
// 	*storage.Model
// }
//
// // New creates a new collection model for organizations
// func (m *OrganizationModel) New() *schema.Organization {
// 	return storage.CreateDocument(&schema.Organization{}).(*schema.Organization)
// }
//
// // FindByID returns an Organization by Id
// func (m *OrganizationModel) FindByID(id interface{}) (*schema.Organization, error) {
// 	org := &schema.Organization{}
// 	err := m.FindId(utils.ObjectId(id)).One(org)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return org, nil
// }
//
// // FindAllIds returns all Organizations by list of ids
// func (m *OrganizationModel) FindAllIds(ids []bson.ObjectId) ([]*schema.Organization, error) {
// 	// objIds := []bson.ObjectId{}
// 	// for _, id := range ids {
// 	// 	objIds = append(objIds, utils.ObjectId(id))
// 	// }
//
// 	orgs := []*schema.Organization{}
// 	err := m.Find(bson.M{"_id": bson.M{"$in": ids}}).All(&orgs)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return orgs, nil
// }

// func init() {
// 	Organization = storage.RegisterModel(schema.Organization{}, "organizations", func(col *mgo.Collection) interface{} {
// 		return &OrganizationModel{storage.NewModel(col)}
// 	}).(*OrganizationModel)
//
// 	// create required indexes in MongoDB
// 	indexes := []mgo.Index{}
//
// 	indexes = append(indexes, mgo.Index{
// 		Key:        []string{"namespace.id"},
// 		Unique:     true,
// 		DropDups:   true,
// 		Background: true, // See notes.
// 		Sparse:     false,
// 	})
//
// 	for _, index := range indexes {
// 		err := Organization.EnsureIndex(index)
// 		if err != nil {
// 			log.Panicf("Failed to ensure index on 'users' Collection: %v", index)
// 		}
// 	}
//
// }
