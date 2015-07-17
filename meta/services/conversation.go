package services

import (
	"github.com/messagedb/messagedb/meta/bindings"
	"github.com/messagedb/messagedb/meta/schema"
)

// ConversationService is responsible for all related actions and properties for
// a single Conversation
type ConversationService struct {
	Conversation *schema.Conversation
	CurrentUser  *schema.User
}

// NewConversationService creates a service that wraps an organization and the current user making API requests
func NewConversationService(conversationParam interface{}, currentUser *schema.User) (*ConversationService, error) {
	// var conversation *schema.Conversation
	// var ok bool
	// var err error
	//
	// if conversation, ok = conversationParam.(*schema.Conversation); !ok {
	// 	conversationID := utils.ObjectId(conversationParam)
	// 	conversation, err = models.Conversation.FindByID(conversationID)
	// 	if err != nil {
	// 		log.Errorf("Unable to find conversation when creating ConversationService: %v", conversationID)
	// 		return nil, err
	// 	}
	// }
	//
	// service := &ConversationService{
	// 	Conversation: conversation,
	// 	CurrentUser:  currentUser,
	// }
	//
	// return service, nil
	return nil, nil
}

// GetConversation retrieves the conversation
func (s *ConversationService) GetConversation() (*schema.Conversation, error) {
	return s.Conversation, nil
}

// ListPublicConversations returns all the conversations
func ListPublicConversations(options interface{}) ([]*schema.Conversation, error) {
	// var err error

	//TODO: handle pagination via options (eg. page, per_page, sort)

	// var conversations []*schema.Conversation
	//
	// err = models.Conversation.Find(nil).All(conversations)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// return conversations, err
	return nil, nil
}

// CreateConversation creates a new conversation in the namespace
func CreateConversation(namespaceID int, json bindings.CreateConversation) (*schema.Conversation, error) {

	// conversation := models.Conversation.New()
	// conversation.Title = json.Title
	// conversation.Purpose = json.Purpose
	//
	// namespace, err := models.Namespace.FindByID(namespaceID)
	// if err != nil {
	// 	log.Errorf("Unable to find namespace: %v", namespaceID)
	// 	return nil, err
	// }
	//
	// conversation.Namespace.ID = namespace.ID
	// conversation.Namespace.Path = namespace.Path
	// conversation.Namespace.OwnerType = namespace.OwnerType
	// conversation.Namespace.OwnerID = namespace.OwnerID
	//
	// err = conversation.Save()
	// if err != nil {
	// 	log.Errorf("Unable to create conversation: %v", err)
	// 	return nil, err
	// }
	//
	// return conversation, nil

	return nil, nil
}
