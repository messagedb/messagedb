package models

// import (
// 	"github.com/messagedb/messagedb/meta/schema"
//
// 	"gopkg.in/mgo.v2/bson"
// )
//
// var Integration *IntegrationModel
//
// // func init() {
// // 	Integration = storage.RegisterModel(schema.Integration{}, "integrations", func(col *mgo.Collection) interface{} {
// // 		return &IntegrationModel{storage.NewModel(col)}
// // 	}).(*IntegrationModel)
// // }
//
// type IntegrationModel struct {
// 	*storage.Model
// }
//
// func (m *IntegrationModel) New() *schema.Integration {
// 	return storage.CreateDocument(&schema.Integration{}).(*schema.Integration)
// }
//
// func (m *IntegrationModel) FindById(id string) (*schema.Integration, error) {
// 	integration := &schema.Integration{}
// 	err := m.FindId(id).One(integration)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return integration, nil
// }
//
// func (m *IntegrationModel) FindByName(name string) (*schema.Integration, error) {
// 	integration := &schema.Integration{}
// 	err := m.Find(bson.M{"name": name}).One(integration)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return integration, nil
// }
