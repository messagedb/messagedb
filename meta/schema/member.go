package schema

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

// MemberRole describes the Membership role
type MemberRole int

// Member roles
const (
	MemberRoleOwner MemberRole = iota
	MemberRoleMember
	MemberRoleGuest
)

var roles = [...]string{"owner", "member", "guest"}

func (m MemberRole) String() string {
	return roles[m]
}

// Member represents an organization membership for a user
type Member struct {
	ID               bson.ObjectId `bson:"_id,omitempty"`
	UserID           bson.ObjectId `bson:"user_id"`
	OrganizationID   bson.ObjectId `bson:"org_id"`
	State            string        `bson:"state"`
	Published        bool          `bson:"published"`
	Role             MemberRole    `bson:"role"`
	InvitedBy        bson.ObjectId `bson:"invited_by,omitempty"`
	InviteEmail      string        `bson:"invite_email,omitempty"`
	InviteToken      string        `bson:"invite_token,omitempty"`
	InviteSentAt     time.Time     `bson:"invite_sent_at,omitempty"`
	InviteAcceptedAt time.Time     `bson:"invite_accepted_at,omitempty"`
	CreatedAt        time.Time     `bson:"created_at"`
	UpdatedAt        time.Time     `bson:"updated_at"`

	Errors Errors `json:"-" bson:"-"`
}

// IsPending returns if the organization membership state is pending
func (m *Member) IsPending() bool { return m.State == "pending" }

// IsActive returns if the organization membership state is active
func (m *Member) IsActive() bool { return m.State == "active" }

// IsPublished returns if the organization membership visibility is public
func (m *Member) IsPublished() bool { return m.Published }

// IsPrivate returns if the organization membership visibility is concealed
func (m *Member) IsPrivate() bool { return !m.Published }

// IsOwner retusn if the user is part of the Owners role group
func (m *Member) IsOwner() bool { return m.Role == MemberRoleOwner }

// IsMember returns if the user is part of the Member role group
func (m *Member) IsMember() bool { return m.Role == MemberRoleMember }

// IsGuest returns if the user is part of the Guest member role group
func (m *Member) IsGuest() bool { return m.Role == MemberRoleGuest }
