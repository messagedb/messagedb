package services

import (
	"errors"

	"github.com/messagedb/messagedb/meta/bindings"
	"github.com/messagedb/messagedb/meta/schema"
)

// Errors
var (
	ErrAuthenticationFailedUserNotFound     = errors.New("Authentication failed. User does not exists")
	ErrAuthenticationFailedValidationError  = errors.New("Authentication failed. Validation Error")
	ErrAuthenticationFailedPasswordMismatch = errors.New("Authentication failed. Password does not match")
)

// AccountService is a service that allows actions on the authenticated user
type AccountService struct {
	User *schema.User
}

// NewAccountService creates a new service for authenticated user
func NewAccountService(userParam interface{}) (*AccountService, error) {
	// var user *schema.User
	// var ok bool
	// var err error
	//
	// if user, ok = userParam.(*schema.User); !ok {
	// 	userid := utils.ObjectId(userParam)
	// 	user, err = models.User.FindById(userid)
	// 	if err != nil {
	// 		log.Errorf("Unable to find user when creating AccountService: %v", userid)
	// 		return nil, err
	// 	}
	// }
	//
	// service := &AccountService{User: user}
	//
	// return service, nil
	return nil, nil
}

// ChangePassword updates the authenticated user's password
func (s *AccountService) ChangePassword(form bindings.ChangePassword) (bool, error) {
	//TODO: fix me
	return false, nil
}

// ChangeUsername updates the authenticated users' username
func (s *AccountService) ChangeUsername(form bindings.ChangeUsername) (bool, error) {
	//TODO: fix me
	return false, nil
}

// AddEmailAddress adds a new email address to the user's account
func (s *AccountService) AddEmailAddress(form bindings.UpdateEmail) error {
	// TODO: fix this
	// s.User.AddEmailAddress(form.Email)
	// err := s.User.Save()
	// if err != nil {
	// 	return err
	// }
	return nil
}

// RemoveEmailAddress removes an email existing email address from the user's account
func (s *AccountService) RemoveEmailAddress(form bindings.UpdateEmail) error {
	// TODO: fix this
	// s.User.RemoveEmailAddress(form.Email)
	// err := s.User.Save()
	// if err != nil {
	// 	return err
	// }
	return nil
}

// ListMyMemberships returns all active memberships for the authenticated user
func (s *AccountService) ListMyMemberships() ([]*schema.Member, error) {
	members := []*schema.Member{}
	// err := models.Member.Find(bson.M{"user_id": s.User.ID, "status": "active"}).All(&members)
	// if err != nil {
	// 	return nil, err
	// }
	return members, nil
}

// ListMyOrganizations returns all active organizations for the authenticated user
func (s *AccountService) ListMyOrganizations() ([]*schema.Organization, error) {
	// TODO: fix this
	// members, err := s.ListMyMemberships()
	// if err != nil {
	// 	return nil, err
	// }
	//
	// orgIds := []bson.ObjectId{}
	// for _, member := range members {
	// 	orgIds = append(orgIds, member.OrganizationID)
	// }
	//
	// orgs, err := models.Organization.FindAllIds(orgIds)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// return orgs, nil
	return nil, nil
}

// GetMyMembership returns the membership for an organization that the authenticated user is a member of
func (s *AccountService) GetMyMembership(orgID interface{}) (*schema.Member, error) {
	member := &schema.Member{}
	//TODO: fix this
	// err := models.Member.Find(bson.M{"org_id": utils.ObjectId(orgID), "user_id": s.User.ID}).One(&member)
	// if err != nil {
	// 	return nil, err
	// }
	return member, nil
}

// EditMyMembership edits the membership for an organization that the authenticated user is a member of
func (s *AccountService) EditMyMembership(orgID interface{}, json bindings.EditMyMembership) (*schema.Member, error) {
	member := &schema.Member{}
	// TODO: Fix this
	// err := models.Member.Find(bson.M{"org_id": utils.ObjectId(orgID), "user_id": s.User.ID}).One(&member)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// member.State = json.State
	// err = member.Save()
	// if err != nil {
	// 	log.Errorf("Failed to save membership: %v", err)
	// 	return nil, err
	// }

	return member, nil
}
