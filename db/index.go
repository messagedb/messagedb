package db

import (
	"regexp"
	"sync"
)

// DatabaseIndex is the in memory index of a collection of conversations, and their tags.
// Exported functions are goroutine safe while un-exported functions assume the caller will use the appropriate locks
type DatabaseIndex struct {
	// in memory metadata index, built on load and updated when new series come in
	mu            sync.RWMutex
	conversations map[string]*Conversation // map conversations key to the Conversations object
	names         []string                 // sorted list of the conversations names
	lastID        uint64                   // last used conversations ID. They're in memory only for this shard
}

// NewDatabaseIndex creates the in memory index
func NewDatabaseIndex() *DatabaseIndex {
	return &DatabaseIndex{
		conversations: make(map[string]*Conversation),
		names:         make([]string, 0),
	}
}

// Conversation returns the measurement object from the index by the name
func (db *DatabaseIndex) Conversation(name string) *Conversation {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.conversations[name]
}

// ConversationsCount returns the number of conversations currently indexed by the database.
// Useful for reporting and monitoring.
func (db *DatabaseIndex) ConversationsCount() (nConversations int) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	nConversations = len(db.conversations)
	return
}

// createSeriesIndexIfNotExists adds the series for the given measurement to the index and sets its ID or returns the existing series object
func (db *DatabaseIndex) createConversationIndexIfNotExists(name string, conversation *Conversation) *Conversation {
	// if there is a measurement for this id, it's already been added
	cc := db.conversations[conversation.Key]
	if cc != nil {
		return cc
	}

	// set the in memory ID for query processing on this shard
	conversation.id = db.lastID + 1
	db.lastID++

	db.conversations[conversation.Key] = conversation

	return conversation
}

// conversationsByRegex returns the conversations that match the regex.
func (db *DatabaseIndex) conversationsByRegex(re *regexp.Regexp) Conversations {
	var matches Conversations
	for _, c := range db.conversations {
		if re.MatchString(c.Name) {
			matches = append(matches, c)
		}
	}
	return matches
}

// Conversations returns a list of all conversations.
func (db *DatabaseIndex) Conversations() Conversations {
	conversations := make(Conversations, 0, len(db.conversations))
	for _, m := range db.conversations {
		conversations = append(conversations, m)
	}
	return conversations
}

func (db *DatabaseIndex) DropConversation(name string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	c := db.conversations[name]
	if c == nil {
		return
	}

	delete(db.conversations, name)

	var names []string
	for _, n := range db.names {
		if n != name {
			names = append(names, n)
		}
	}
	db.names = names
}
