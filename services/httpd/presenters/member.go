package presenters

import (
	"time"

	"github.com/messagedb/messagedb/meta/schema"
)

// Member presents the schema.Member record that is return to the API responses
type Member struct {
	ID               string    `json:"id"`
	UserID           string    `json:"user_id"`
	OrganizationID   string    `json:"org_id"`
	State            string    `json:"status"`
	Published        bool      `json:"published"`
	InvitedBy        string    `json:"invited_by,omitempty"`
	InviteEmail      string    `json:"invite_email,omitempty"`
	InviteSentAt     time.Time `json:"invite_sent_at,omitempty"`
	InviteAcceptedAt time.Time `json:"invite_accepted_at,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// MemberPresenter creates a new instance of the Member presenter
func MemberPresenter(m *schema.Member) *Member {
	member := &Member{}
	member.ID = m.ID.Hex()
	member.UserID = m.UserID.Hex()
	member.OrganizationID = m.OrganizationID.Hex()
	member.State = m.State
	member.Published = m.Published
	member.InvitedBy = m.InvitedBy.Hex()
	member.InviteEmail = m.InviteEmail
	member.InviteSentAt = m.InviteSentAt
	member.InviteAcceptedAt = m.InviteAcceptedAt
	member.CreatedAt = m.CreatedAt
	member.UpdatedAt = m.UpdatedAt

	return member
}

// MemberCollectionPresenter creates a collection of presenters for Member
func MemberCollectionPresenter(items []*schema.Member) []*Member {
	var collection []*Member
	for _, item := range items {
		collection = append(collection, MemberPresenter(item))
	}
	return collection
}
