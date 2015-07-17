package presenters

import (
	"fmt"
	"net/url"

	"github.com/messagedb/messagedb/meta/schema"
)

// User represents a API user.
type User struct {
	ID           string `json:"id"`
	Username     string `json:"username"`
	FullName     string `json:"full_name,omitempty"`
	PrimaryEmail string `json:"primary_email,omitempty"`

	Namespace struct {
		ID        string `json:"id,omitempty"`
		Path      string `json:"path,omitempty"`
		OwnerType string `json:"owner_type"`
	} `json:"namespace,omitempty"`
}

// GetLocation returns the API location for the user resource
func (u *User) GetLocation() *url.URL {
	uri, err := url.Parse(fmt.Sprintf("/users/%s", u.ID))
	if err != nil {
		return nil
	}
	return uri
}

// UserPresenter creates a new instance of the presenter for the User model
func UserPresenter(u *schema.User) *User {
	user := &User{}
	user.ID = u.ID.Hex()
	user.Username = u.Username
	user.FullName = u.FullName()
	user.PrimaryEmail = u.GetPrimaryEmail()

	user.Namespace.ID = u.Namespace.ID.Hex()
	user.Namespace.Path = u.Namespace.Path

	return user
}

// UserCollectionPresenter creates an array of presenters for the User model
func UserCollectionPresenter(items []*schema.User) []*User {
	var collection []*User
	for _, item := range items {
		collection = append(collection, UserPresenter(item))
	}
	return collection
}
