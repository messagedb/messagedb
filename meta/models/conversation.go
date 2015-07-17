package models

// import (
// 	"github.com/messagedb/messagedb/meta/schema"
// 	"github.com/messagedb/messagedb/meta/utils"
//
// 	"gopkg.in/mgo.v2/bson"
// )
//
// // Instance
// var Conversation *ConversationModel
//
// // ConversationModel represents the storage model for a schema.Conversation
// type ConversationModel struct {
// 	*storage.Model
// }
//
// // New creates a new Conversation model
// func (m *ConversationModel) New() *schema.Conversation {
// 	conversation := schema.NewConversation()
// 	return storage.CreateDocument(conversation).(*schema.Conversation)
// }
//
// // FindByID returns a conversation based on the ID provided
// func (m *ConversationModel) FindByID(id interface{}) (*schema.Conversation, error) {
// 	conversation := &schema.Conversation{}
// 	err := m.FindId(utils.ObjectId(id)).One(conversation)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return conversation, nil
// }
//
// // FindAllByNamespaceID returns all the conversations that belongs to the namespace ID
// func (m *ConversationModel) FindAllByNamespaceID(namespaceID interface{}) ([]*schema.Conversation, error) {
// 	var conversations []*schema.Conversation
// 	err := m.Find(bson.M{"namespace.id": utils.ObjectId(namespaceID)}).All(&conversations)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return conversations, nil
// }
//
// // FindAllByNamespacePath returns all the conversations that belongs to the namespace path
// func (m *ConversationModel) FindAllByNamespacePath(namespacePath string) ([]*schema.Conversation, error) {
// 	var conversations []*schema.Conversation
// 	err := m.Find(bson.M{"namespace.path": namespacePath}).All(&conversations)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return conversations, nil
// }

// func init() {
// 	Conversation = storage.RegisterModel(schema.Conversation{}, "conversations", func(col *mgo.Collection) interface{} {
// 		return &ConversationModel{storage.NewModel(col)}
// 	}).(*ConversationModel)
//
// 	// create required indexes in MongoDB
// 	indexes := []mgo.Index{}
//
// 	indexes = append(indexes, mgo.Index{
// 		Key:        []string{"namespace.id, conversation_type, last_active_at"},
// 		Unique:     false,
// 		DropDups:   false,
// 		Background: true, // See notes.
// 		Sparse:     false,
// 	})
//
// 	indexes = append(indexes, mgo.Index{
// 		Key:        []string{"namespace.path, conversation_type, last_active_at"},
// 		Unique:     false,
// 		DropDups:   false,
// 		Background: true, // See notes.
// 		Sparse:     false,
// 	})
//
// 	indexes = append(indexes, mgo.Index{
// 		Key:        []string{"title"},
// 		Unique:     false,
// 		DropDups:   false,
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
