package bindings

// CreateOrganization is the API payload representation when creating a new Organization
type CreateConversation struct {
	Title   string `json:"title" binding:"required"`
	Purpose string `json:"purpose" binding:"required"`
}
