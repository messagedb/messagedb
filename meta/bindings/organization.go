package bindings

// CreateOrganization is the API payload representation when creating a new Organization
type CreateOrganization struct {
	Path         string `json:"path" binding:"required"`
	BillingEmail string `json:"billing_email" binding:"required"`
}

// UpdateOrganization is the API payload representation when updating an Organizations
type UpdateOrganization struct {
	Name         string `json:"name" binding:"required"`
	BillingEmail string `json:"billing_email" binding:"required"`
	Email        string `json:"email"`
	Description  string `json:"description"`
	URL          string `json:"url"`
	Location     string `json:"location"`
}

// AddUpdateMembership is the API payload representation when adding or updating a Membership within an organization
type AddUpdateMembership struct {
	Role string `json:"role" binding:"required"`
}

// EditMyMembership is the API payload representation when editing your own membership for an organization
type EditMyMembership struct {
	State string `json:"state" binding:"required"`
}
