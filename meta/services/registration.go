package services

import (
	"errors"

	"github.com/messagedb/messagedb/meta/bindings"
	"github.com/messagedb/messagedb/meta/schema"
)

// Errors
var (
	ErrNamespaceAlreadyExists = errors.New("Namespace already exists")
	ErrNamespaceDuplicateKey  = errors.New("Namespace duplicate key error")
	ErrUserDuplicateKey       = errors.New("Duplicate Key for Organization")
)

// RegisterNewUser creates a new user account
func RegisterNewUser(newUser bindings.RegisterNewUser) (*schema.User, error) {

	// var err error
	//
	// namespace, err := models.Namespace.FindByPath(newUser.Username)
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
	// namespace.Path = strings.ToLower(newUser.Username)
	// namespace.OwnerType = schema.OwnerTypeUser
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
	// // Creates a new user
	// user := models.User.New()
	// user.Namespace.ID = namespace.ID
	// user.Namespace.Path = namespace.Path
	// user.Username = namespace.Path
	// user.SetPrimaryEmail(newUser.EmailAddress)
	// user.SetPassword(newUser.Password)
	// err = user.Save()
	// if err != nil {
	// 	if mgo.IsDup(err) {
	// 		log.Errorf("Duplicate key when saving user %v", user)
	// 		return nil, ErrUserDuplicateKey
	// 	}
	// 	log.Errorf("Error saving User %v", user)
	// 	return nil, err
	// }
	//
	// // associate the new user with the namespace owner_id
	// namespace.OwnerID = user.ID
	// err = namespace.Save()
	// if err != nil {
	// 	if mgo.IsDup(err) {
	// 		log.Errorf("Duplicate key when saving namespace %v", namespace)
	// 		return nil, ErrNamespaceDuplicateKey
	// 	}
	// 	log.Errorf("Error saving Namespace %v", namespace)
	// 	return nil, err
	// }
	//
	// return user, nil
	return nil, nil
}
