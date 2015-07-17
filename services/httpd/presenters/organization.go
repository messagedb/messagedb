package presenters

import (
	"fmt"
	"net/url"
	"time"

	"github.com/messagedb/messagedb/meta/schema"
)

// Organization is a presenter for the schema.Organization model
type Organization struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	URL         string    `json:"url"`
	Location    string    `json:"location"`
	Email       string    `json:"email"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	Namespace struct {
		ID        string `json:"id,omitempty"`
		Path      string `json:"path,omitempty"`
		OwnerType string `json:"owner_type"`
	} `json:"namespace,omitempty"`
}

// GetLocation returns the API location for the organization resource
func (o *Organization) GetLocation() *url.URL {
	uri, err := url.Parse(fmt.Sprintf("/orgs/%s", o.ID))
	if err != nil {
		return nil
	}
	return uri
}

// OrganizationPresenter creates a new instance of the presenter for the Organization model
func OrganizationPresenter(o *schema.Organization) *Organization {
	org := &Organization{}
	org.ID = o.ID.Hex()
	org.Name = o.Name
	org.Description = o.Description
	org.URL = o.URL
	org.Location = o.Location
	org.Email = o.Email
	org.CreatedAt = o.CreatedAt
	org.UpdatedAt = o.UpdatedAt

	org.Namespace.ID = o.Namespace.ID.Hex()
	org.Namespace.Path = o.Namespace.Path

	return org
}

// OrganizationCollectionPresenter creates an array of presenters for the Organization model
func OrganizationCollectionPresenter(items []*schema.Organization) []*Organization {
	var collection []*Organization
	for _, item := range items {
		collection = append(collection, OrganizationPresenter(item))
	}
	return collection
}
