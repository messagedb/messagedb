package services

import (
	"errors"

	"github.com/messagedb/messagedb/meta/bindings"
	"github.com/messagedb/messagedb/meta/schema"
)

var (
	// ErrNotAnOrganizationOwner is raised when a user that is not an organization member tries to perform an action
	ErrNotAnOrganizationOwner = errors.New("Authenticated user is not an organization owner")

	// ErrCannotChangeMembershipVisibility is raised when a user tries to change membership visibility of another user
	ErrCannotChangeMembershipVisibility = errors.New("Cannot change other member's membership visibility")
)

// OrganizationMembershipService is a service that allows actions on an organization's membership
type OrganizationMembershipService struct {
	Org         *schema.Organization
	CurrentUser *schema.User
}

// GetMembership returns the Member record for the provided user
func (s *OrganizationMembershipService) GetMembership(user *schema.User) (*schema.Member, error) {

	currentMember, err := s.GetCurrentMembership()
	if err != nil {
		return nil, err
	}

	// if authenticated user is not an organization owner raise an error
	if !currentMember.IsOwner() {
		return nil, ErrNotAnOrganizationOwner
	}

	// return models.Member.FindByUserID(s.Org.ID, user.ID)
	return nil, nil //TODO: fix this
}

// GetCurrentMembership returns the organization membership for the authenticated user
func (s *OrganizationMembershipService) GetCurrentMembership() (*schema.Member, error) {
	// return models.Member.FindByUserID(s.Org.ID, s.CurrentUser)
	return nil, nil //TODO: fix this
}

// GetMembers returns the list of all members of the organization
func (s *OrganizationMembershipService) GetMembers() ([]*schema.Member, error) {
	members := []*schema.Member{}
	//TODO: fix this
	// err := models.Member.Find(bson.M{"org_id": s.Org.ID}).All(&members)
	// if err != nil {
	// 	return nil, err
	// }
	return members, nil
}

// GetPublicMembers returns the organization public member
func (s *OrganizationMembershipService) GetPublicMembers() ([]*schema.Member, error) {
	members := []*schema.Member{}
	//TODO: fix this
	// err := models.Member.Find(bson.M{"org_id": s.Org.ID, "published": true}).All(&members)
	// if err != nil {
	// 	return nil, err
	// }
	return members, nil
}

// CheckMembershipByUsername verifies if the provided username has a membership in the organization
func (s *OrganizationMembershipService) CheckMembershipByUsername(username string) (bool, error) {
	// user, _ := models.User.FindByUsername(username)
	// return s.CheckMembership(user)
	return false, nil
}

// CheckMembership verifies if the provided user has a membership in the organization
func (s *OrganizationMembershipService) CheckMembership(user *schema.User) (bool, error) {

	//TODO: fix this
	// member, err := models.Member.FindByUserID(s.Org.ID, user.ID)
	// if err != nil {
	// 	if err == storage.ErrNotFound {
	// 		return false, nil
	// 	}
	// 	return false, err
	// }
	//
	// if member.IsPending() {
	// 	return false, nil
	// }

	return true, nil
}

// CheckPublicMembership verifies if the provided user has a public membership in the organization
func (s *OrganizationMembershipService) CheckPublicMembership(user *schema.User) (bool, error) {

	//TODO: fix this
	// member, err := models.Member.FindByUserID(s.Org.ID, user.ID)
	// if err != nil {
	// 	if err == storage.ErrNotFound {
	// 		return false, nil
	// 	}
	// 	return false, err
	// }
	//
	// // also check if membership has been published
	// if member.IsPending() || member.IsPrivate() {
	// 	return false, nil
	// }

	return true, nil
}

// PublicizeMembership publishes the authenticated user membership
func (s *OrganizationMembershipService) PublicizeMembership(user *schema.User) error {

	// user can only change it's own membership visibility
	if s.CurrentUser.ID != user.ID {
		return ErrCannotChangeMembershipVisibility
	}

	//TODO: fix this
	// member, err := models.Member.FindByUserID(s.Org.ID, user.ID)
	// if err != nil {
	// 	return err
	// }
	//
	// if !member.Published {
	// 	member.Published = true
	// 	return member.Save()
	// }

	return nil
}

// ConcealMembership conceals the authenticated user membership
func (s *OrganizationMembershipService) ConcealMembership(user *schema.User) error {

	// user can only change it's own membership visibility
	if s.CurrentUser.ID != user.ID {
		return ErrCannotChangeMembershipVisibility
	}

	// TODO: fix this
	// member, err := models.Member.FindByUserID(s.Org.ID, user.ID)
	// if err != nil {
	// 	return err
	// }
	//
	// if member.Published {
	// 	member.Published = false
	// 	return member.Save()
	// }

	return nil
}

// AddOrUpdateMembership adds or updates the membership for the user in the organization
func (s *OrganizationMembershipService) AddOrUpdateMembership(user *schema.User, form bindings.AddUpdateMembership) (*schema.Member, error) {

	currentMember, err := s.GetCurrentMembership()
	if err != nil {
		return nil, err
	}

	// if authenticated user is not an organization owner raise an error
	if !currentMember.IsOwner() {
		return nil, ErrNotAnOrganizationOwner
	}

	// TODO: fix this
	// found := false
	// member, err := models.Member.FindByUserID(s.Org.ID, user.ID)
	// if err != nil {
	// 	if err == storage.ErrNotFound {
	// 		found = true
	// 	} else {
	// 		return nil, err
	// 	}
	// }
	//
	// if !found {
	// 	member := models.Member.New()
	// 	member.UserID = user.ID
	// 	member.OrganizationID = s.Org.ID
	// }
	//
	// if form.Role == "owner" {
	// 	member.Role = schema.MemberRoleOwner
	// } else {
	// 	member.Role = schema.MemberRoleMember
	// }
	//
	// err = member.Save()
	// if err != nil {
	// 	return nil, err
	// }
	//
	// return member, nil
	return nil, nil
}

// RemoveMembership removes the user from the organization
func (s *OrganizationMembershipService) RemoveMembership(user *schema.User) error {

	//TODO: don't allow to remove current user's own ownership

	currentMember, err := s.GetCurrentMembership()
	if err != nil {
		return err
	}

	// if authenticated user is not an organization owner raise an error
	if !currentMember.IsOwner() {
		return ErrNotAnOrganizationOwner
	}

	// TODO: fix this
	// member, err := models.Member.FindByUserID(s.Org.ID, user.ID)
	// if err != nil {
	// 	return err
	// }
	//
	// err = models.Member.RemoveID(member.ID)
	// if err != nil {
	// 	return err
	// }
	//
	return nil
}
