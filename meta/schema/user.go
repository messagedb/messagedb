package schema

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/mgo.v2/bson"
)

// Errors
var (
	ErrUserAlreadyExist = errors.New("User already exist")
	ErrUserNotExist     = errors.New("User does not exist")
	ErrUserNotValid     = errors.New("User is not valid")
)

// User represents the object of individual and member of organization.
type User struct {
	ID bson.ObjectId `json:"id" bson:"_id,omitempty"`

	Namespace struct {
		ID        bson.ObjectId `bson:"id"`
		Path      string        `bson:"path"`
		OwnerType OwnerType     `bson:"owner_type"`
	} `bson:"namespace"`

	Username       string         `json:"username" bson:"username"`
	GivenName      string         `json:"given_name,omitempty" bson:"given_name,omitempty"`
	FamilyName     string         `json:"family_name,omitempty" bson:"family_name,omitempty"`
	HashedPassword string         `json:"-" bson:"hashed_password"`
	primaryEmail   string         `bson:"primary_email"`
	Emails         []EmailAddress `json:"emails" bson:"emails"`
	CreatedAt      time.Time      `json:"created_at" bson:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at" bson:"updated_at"`
	Errors         Errors         `json:"-" bson:"-"`
}

// EmailAddress is the list of all email addresses of a user. Can contain the
// primary email address, but is not obligatory
type EmailAddress struct {
	Email       string    `json:"email" bson:"email"`
	IsConfirmed bool      `json:"confirmed" bson:"confirmed"`
	ConfirmedAt time.Time `json:"confirmed_at" bson:"confirmed_at"`
	DeletedAt   time.Time `json:"deleted_at" bson:"deleted_at"`
}

// func (u *User) insert() error {
// 	u.CreatedAt = time.Now()
// 	u.UpdatedAt = u.CreatedAt
// 	return UsersCollection().Insert(u)
// }

// func (u *User) update() error {
// 	u.UpdatedAt = time.Now()
// 	return UsersCollection().UpdateId(u.Id, u)
// }

// func (u *User) ErrorMessages() map[string]interface{} {
// 	errorMessages := map[string]interface{}{}
// 	for fieldName, message := range u.Errors.Messages {
// 		errorMessages[fieldName] = message
// 	}
// 	return errorMessages
// }

// func (u *User) IsValid() bool {
// 	u.Errors.Clear()

// 	//TODO: perform validation here

// 	return !u.Errors.HasMessages()
// }

// SetPassword updates the password for the user
func (u *User) SetPassword(password string) error {
	hashBytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	u.HashedPassword = string(hashBytes)
	return nil
}

// ValidatePassword verifies if the provided password matches the one in the database
func (u *User) ValidatePassword(password string) (bool, error) {
	err := bcrypt.CompareHashAndPassword([]byte(u.HashedPassword), []byte(password))
	if err != nil {
		return false, err
	}
	return true, nil
}

// ListOfEmails returns the list of email address that the user have registered in the system
func (u *User) ListOfEmails() []string {
	var emails []string
	for _, item := range u.Emails {
		emails = append(emails, item.Email)
	}
	return emails
}

// SetPrimaryEmail sets the provided email address as the primary email for the account
func (u *User) SetPrimaryEmail(email string) {
	u.primaryEmail = email
	u.AddEmailAddress(email)
}

// GetPrimaryEmail returns the user's primary email address
func (u *User) GetPrimaryEmail() string {
	return u.primaryEmail
}

// HasEmailAddress returns true if the user has the provided email address registered already
func (u *User) HasEmailAddress(email string) bool {
	for _, item := range u.Emails {
		if strings.ToLower(item.Email) == email {
			return true
		}
	}
	return false
}

// AddEmailAddress adds a new email address to the user's account
func (u *User) AddEmailAddress(email string) {
	email = strings.ToLower(email)
	for _, item := range u.Emails {
		if strings.ToLower(item.Email) == email {
			return // found so bail
		}
	}
	u.Emails = append(u.Emails, EmailAddress{Email: email, IsConfirmed: false})
}

// RemoveEmailAddress removes the email address from the user's account
func (u *User) RemoveEmailAddress(email string) {
	email = strings.ToLower(email)
	for i, item := range u.Emails {
		if strings.ToLower(item.Email) == email {
			u.Emails = append(u.Emails[:i], u.Emails[i+1:]...)
			break
		}
	}
}

// FullName returns the full user's name
func (u *User) FullName() string {
	if len(u.GivenName) > 0 && len(u.FamilyName) > 0 {
		return fmt.Sprintf("%s %s", u.GivenName, u.FamilyName)
	} else if len(u.GivenName) > 0 {
		return u.GivenName
	} else if len(u.FamilyName) > 0 {
		return u.FamilyName
	}

	return ""
}

// AbbreviatedName returns the abbreviated name for the user
func (u *User) AbbreviatedName() string {
	return fmt.Sprintf("%s %s.", u.GivenName, string(u.FamilyName[0]))
}
