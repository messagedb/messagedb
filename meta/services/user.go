package services

import "github.com/messagedb/messagedb/meta/schema"

// UserService is a service that allows actions on specific user
type UserService struct {
	User *schema.User
}

// NewUserService creates a new service for authenticated user
func NewUserService(userParam interface{}) (*UserService, error) {

	// var user *schema.User
	// var ok bool
	// var err error
	//
	// if user, ok = userParam.(*schema.User); !ok {
	// 	userid := utils.ObjectId(userParam)
	// 	user, err = models.User.FindById(userid)
	// 	if err != nil {
	// 		log.Errorf("Unable to find user when creating UserService: %v", userid)
	// 		return nil, err
	// 	}
	// }
	//
	// service := &UserService{User: user}
	//
	// return service, nil
	return nil, nil
}

// ListMemberships returns all public memberships for the user
func (s *UserService) ListMemberships() ([]*schema.Member, error) {
	members := []*schema.Member{}
	//TODO: fix this
	// err := models.Member.Find(bson.M{"user_id": s.User.ID, "status": "active", "published": true}).All(&members)
	// if err != nil {
	// 	return nil, err
	// }
	return members, nil
}

// ListOrganizations returns all the active publicized organizations for the user
func (s *UserService) ListOrganizations() ([]*schema.Organization, error) {
	// members, err := s.ListMemberships()
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

// ListAllUsers returns a list of all users
func ListAllUsers() ([]*schema.User, error) {
	users := []*schema.User{}
	// err := models.User.Find(nil).All(&users)
	// if err != nil {
	// 	return nil, err
	// }
	return users, nil
}
