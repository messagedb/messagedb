package models

//
// import (
// 	"github.com/messagedb/messagedb/meta/schema"
// )
//
// var Message *MessageModel
//
// // func init() {
// // 	Message = storage.RegisterModel(schema.Message{}, "messages", func(col *mgo.Collection) interface{} {
// // 		return &MessageModel{storage.NewModel(col)}
// // 	}).(*MessageModel)
// // }
//
// type MessageModel struct {
// 	*storage.Model
// }
//
// func (m *MessageModel) New() *schema.Message {
// 	return storage.CreateDocument(&schema.Message{}).(*schema.Message)
// }
//
// func (m *MessageModel) FindById(id string) (*schema.Message, error) {
// 	messages := &schema.Message{}
// 	err := m.FindId(id).One(messages)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return messages, nil
// }
