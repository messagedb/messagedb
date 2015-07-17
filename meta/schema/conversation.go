//go:generate jsonenums -type=ConversationType -suffix=_enum
//go:generate jsonenums -type=RetentionMode -suffix=_enum
//go:generate jsonenums -type=Privacy -suffix=_enum

package schema

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

// Conversation represents a conversation within a namespace. Conversastions can be 1-on-1, group, or channel.
type Conversation struct {
	ID        bson.ObjectId `bson:"_id,omitempty"`
	CreatorID bson.ObjectId `bson:"creator_id"`
	Title     string        `bson:"title"`
	Purpose   string        `bson:"purpose"`
	Topic     string        `bson:"topic"`

	Namespace struct {
		ID        bson.ObjectId `bson:"id"`
		Path      string        `bson:"path"`
		OwnerID   bson.ObjectId `bson:"owner_id"`
		OwnerType OwnerType     `bson:"owner_type"`
	} `bson:"namespace"`

	ConversationType ConversationType `bson:"conversation_type"`
	Privacy          Privacy          `bson:"privacy,ommitempty"`

	Avatar struct {
		FileSize         int    `bson:"file_size"`
		OriginalFilename string `bson:"original_filename"`
		ContenType       string `bson:"content_type"`
		Md5              string `bson:"md5"`
		Sha256           string `bson:"sha256"`
	}

	Retention struct {
		Mode  RetentionMode `bson:"mode"`
		Value int           `bson:"value,omitempty"`
	} `bson:"retention,omitempty"`

	MessagesCount     int `bson:"messages_count"`
	ParticipantsCount int `bson:"participants_count"`

	LastActiveAt time.Time `bson:"last_active_at"`
	Archived     bool      `bson:"archived"`
	ArchivedAt   time.Time `bson:"archived_at"`
	CreatedAt    time.Time `bson:"created_at"`
	UpdatedAt    time.Time `bson:"updated_at"`

	LastMessage struct {
		ContentHTML      string
		ContentPlainText string

		From struct {
			UserID bson.ObjectId
			Name   string
		} `bson:"from"`

		CreatedAt time.Time `bson:"created_at"`
		UpdatedAt time.Time `bson:"updated_at"`
	} `bson:"last_message,omitempty"`

	Errors Errors `bson:"-"`
}

// NewConversation creates a new instance of a Conversation
func NewConversation() *Conversation {

	conversation := &Conversation{}

	// setup the defaults
	conversation.MessagesCount = 0
	conversation.ParticipantsCount = 0
	conversation.LastActiveAt = time.Now()
	conversation.Archived = false

	return conversation
}

// IsArchived is a helper method that returns if the conversation has been archived
func (c *Conversation) IsArchived() bool {
	return c.Archived
}

// ConversationType represents the one of the 3 modes of conversation supported: private (aka 1-on-1s), group, and channels (aka. Rooms)
type ConversationType int

// Conversation Types
const (
	ConversationTypePrivate ConversationType = iota
	ConversationTypeGroup
	ConversationTypeChannel
)

func (t ConversationType) String() string {
	switch t {
	case ConversationTypePrivate:
		return "private"
	case ConversationTypeGroup:
		return "group"
	case ConversationTypeChannel:
		return "channel"
	default:
		return "invalid conversation type"
	}
}

// Privacy represents the type of privacy and security a conversation has
type Privacy int

// Privacy modes
const (
	PrivacyPersonal Privacy = iota
	PrivacyPublic
	PrivacyPrivate
	PrivacyProtected
	PrivacySecret
)

func (p Privacy) String() string {
	switch p {
	case PrivacyPersonal:
		return "personal"
	case PrivacyPublic:
		return "public"
	case PrivacyPrivate:
		return "private"
	case PrivacyProtected:
		return "protected"
	case PrivacySecret:
		return "secret"
	default:
		return "invalid channel privacy"
	}
}

// RetentionMode describes the type of rentention policy for messages in a conversation
type RetentionMode int

// Retention modes
const (
	RetentionModeAll RetentionMode = iota
	RetentionModeNone
	RetentionModeAge
	RetentionModeDays
)

func (m RetentionMode) String() string {
	switch m {
	case RetentionModeAll:
		return "all"
	case RetentionModeNone:
		return "all"
	case RetentionModeAge:
		return "age"
	case RetentionModeDays:
		return "days"
	default:
		return "invalid"
	}
}
