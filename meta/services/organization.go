package services

import (
	"errors"

	"github.com/messagedb/messagedb/meta/bindings"
	"github.com/messagedb/messagedb/meta/schema"
)

var (
	// ErrOrganizationDuplicateKey is raised when there is an error saving an organization to the database because of unique index conflict
	ErrOrganizationDuplicateKey = errors.New("Duplicate Key for Organization")
)

// OrganizationService is a service that allows operations on an organization. It wraps an organization and the current user making API requests.
// It enforces access-controls based on the current user to make sure it has permissions to perform the desired operations
type OrganizationService struct {
	Org         *schema.Organization
	CurrentUser *schema.User

	*OrganizationMembershipService
}

// NewOrganizationService creates a service that wraps an organization and the current user making API requests
func NewOrganizationService(orgParam interface{}, currentUser *schema.User) (*OrganizationService, error) {
	// var org *schema.Organization
	// var ok bool
	// var err error
	//
	// if org, ok = orgParam.(*schema.Organization); !ok {
	// 	orgid := utils.ObjectId(orgParam)
	// 	org, err = models.Organization.FindByID(orgid)
	// 	if err != nil {
	// 		log.Errorf("Unable to find organization when creating OrganizationService: %v", orgid)
	// 		return nil, err
	// 	}
	// }
	//
	// service := &OrganizationService{
	// 	Org:                           org,
	// 	CurrentUser:                   currentUser,
	// 	OrganizationMembershipService: &OrganizationMembershipService{Org: org, CurrentUser: currentUser},
	// }
	//
	// return service, nil
	return nil, nil
}

// UpdateOrganization modifies the organization wrapped by the service
func (s *OrganizationService) UpdateOrganization(newOrg bindings.UpdateOrganization) (*schema.Organization, error) {

	// TODO: fix this
	// // copy fields from bindings payload into the target object
	// s.Org.Name = newOrg.Name
	// s.Org.BillingEmail = newOrg.BillingEmail
	// s.Org.Email = newOrg.Email
	// s.Org.Description = newOrg.Description
	// s.Org.URL = newOrg.URL
	// s.Org.Location = newOrg.Location
	//
	// err := s.Org.Save()
	// if err != nil {
	// 	log.Errorf("Error saving Organization %v", s.Org)
	// 	return nil, err
	// }
	//
	// return s.Org, nil
	return nil, nil
}

// CreateOrganization creates a new organization and makes the current user the owner of the new organization
func CreateOrganization(newOrg bindings.CreateOrganization, currentUser *schema.User) (*schema.Organization, error) {
	//TODO: fix this
	// var err error
	//
	// namespace, err := models.Namespace.FindByPath(newOrg.Path)
	// if err != nil && err != storage.ErrNotFound {
	// 	return nil, err
	// }
	//
	// // if namespace was found... then return error
	// if namespace != nil {
	// 	log.Errorf("Error Namespace already exists: %v", namespace)
	// 	return nil, ErrNamespaceAlreadyExists
	// }
	//
	// // creates new Namespace
	// namespace = models.Namespace.New()
	// namespace.Path = strings.ToLower(newOrg.Path)
	// namespace.OwnerType = schema.OwnerTypeOrganization
	//
	// err = namespace.Save()
	// if err != nil {
	// 	if mgo.IsDup(err) {
	// 		log.Errorf("Duplicate key when saving namespace %v", namespace)
	// 		return nil, ErrNamespaceDuplicateKey
	// 	}
	// 	return nil, err
	// }
	//
	// // lets create a new Organization instance
	// org := models.Organization.New()
	// org.Namespace.ID = namespace.ID
	// org.Namespace.Path = namespace.Path
	// org.Name = newOrg.Path
	// org.BillingEmail = newOrg.BillingEmail
	//
	// err = org.Save()
	// if err != nil {
	// 	if mgo.IsDup(err) {
	// 		log.Errorf("Duplicate key when saving organization %v", org)
	// 		return nil, ErrOrganizationDuplicateKey
	// 	}
	// 	log.Errorf("Error saving Organization %v", org)
	// 	return nil, err
	// }
	//
	// // associate the new org with the namespace owner_id
	// namespace.OwnerID = org.ID
	//
	// // performs the save to the database
	// err = namespace.Save()
	// if err != nil {
	// 	if mgo.IsDup(err) {
	// 		log.Errorf("Duplicate key when saving namespace %v", namespace)
	// 		return nil, ErrNamespaceDuplicateKey
	// 	}
	// 	log.Errorf("Error saving Namespace %v: %v", namespace, err)
	// 	return nil, err
	// }
	//
	// return org, nil
	return nil, nil
}
