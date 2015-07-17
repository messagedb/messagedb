package presenters

import (
	"fmt"
	"net/url"

	"github.com/messagedb/messagedb/meta/schema"
)

// Conversation is a presenter for the schema.Conversation model
type Conversation struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Purpose string `json:"purpose"`

	Namespace struct {
		ID        string `json:"id,omitempty"`
		Path      string `json:"path,omitempty"`
		OwnerType string `json:"owner_type"`
	} `json:"namespace,omitempty"`
}

// GetLocation returns the API location for the organization resource
func (c *Conversation) GetLocation() *url.URL {
	uri, err := url.Parse(fmt.Sprintf("/conversations/%s", c.ID))
	if err != nil {
		return nil
	}
	return uri
}

// ConversationPresenter creates a new instance of the presenter for the Conversation model
func ConversationPresenter(c *schema.Conversation) *Conversation {
	conversation := &Conversation{}
	conversation.ID = c.ID.Hex()
	conversation.Title = c.Title
	conversation.Purpose = c.Purpose

	conversation.Namespace.ID = c.Namespace.ID.Hex()
	conversation.Namespace.Path = c.Namespace.Path

	return conversation
}

// ConversationCollectionPresenter creates an array of presenters for the Conversation model
func ConversationCollectionPresenter(items []*schema.Conversation) []*Conversation {
	var collection []*Conversation
	for _, item := range items {
		collection = append(collection, ConversationPresenter(item))
	}
	return collection
}
