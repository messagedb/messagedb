package services

// import (
// 	"errors"

// 	"github.com/messagedb/messagedb/meta/bindings"
// 	"github.com/messagedb/messagedb/meta/models"
// 	"github.com/messagedb/messagedb/meta/schema"

// 	log "github.com/Sirupsen/logrus"
// 	"gopkg.in/mgo.v2"
// )

// var (
// 	ErrTeamDuplicateKey               = errors.New("Team duplicate key error")
// 	ErrTeamNameAlreadyExists          = errors.New("Team name already exists")
// 	ErrTeamHasMembers                 = errors.New("Team has members")
// 	ErrForbiddenToDeleteOwnersTeam    = errors.New("Forbiden to delete the organization Owners team")
// 	ErrForbiddenNotOrganizationOwner  = errors.New("User is not part of the Organization Owners")
// 	ErrForbiddenNotOrganizationMember = errors.New("User is not part of the Organization Members")
// )

// var Team *teamService = &teamService{}

// type teamService struct{}

// func (s *teamService) CreateOwnersTeam(org *schema.Organization) (*schema.Team, error) {
// 	newTeam := bindings.CreateUpdateTeam{Name: "Owners", Description: ""}
// 	return s.CreateTeam(org, newTeam, schema.TeamTypeOwners)
// }

// func (s *teamService) CreateAdminsTeam(org *schema.Organization) (*schema.Team, error) {
// 	newTeam := bindings.CreateUpdateTeam{Name: "Admins", Description: ""}
// 	return s.CreateTeam(org, newTeam, schema.TeamTypeAdmins)
// }

// func (s *teamService) CreateTeam(org *schema.Organization, newTeam bindings.CreateUpdateTeam, teamType schema.TeamType) (*schema.Team, error) {
// 	var err error

// 	var existingTeam *schema.Team
// 	if existingTeam, err = s.getExistingTeam(org, newTeam.Name); err != nil {
// 		return nil, err
// 	}

// 	// make sure team name doesn't already exists
// 	if existingTeam != nil {
// 		log.Warnf("Trying creating team with same name: %s", newTeam.Name)
// 		return nil, ErrTeamNameAlreadyExists
// 	}

// 	team := models.Team.New()
// 	team.Name = newTeam.Name
// 	team.Description = newTeam.Description
// 	team.TeamType = teamType
// 	team.OrganizationId = org.Id

// 	err = team.Save()
// 	if err != nil {
// 		if mgo.IsDup(err) {
// 			log.Errorf("Duplicate key when saving team %v", team)
// 			return nil, ErrTeamDuplicateKey
// 		}
// 		log.Errorf("Error saving team %v", team)
// 		return nil, err
// 	}

// 	return team, nil
// }

// func (s *teamService) UpdateTeam(org *schema.Organization, team *schema.Team, newTeam bindings.CreateUpdateTeam) (*schema.Team, error) {
// 	var err error
// 	var existingTeam *schema.Team

// 	if existingTeam, err = s.getExistingTeam(org, newTeam.Name); err != nil {
// 		return nil, err
// 	}

// 	// make sure team name doesn't already exists and doesn't match the team we are trying to edit
// 	if existingTeam != nil && existingTeam.Id != team.Id {
// 		log.Warnf("Trying creating team with same name: %s", newTeam.Name)
// 		return nil, ErrTeamNameAlreadyExists
// 	}

// 	// copy fields from bindings payload into the target object
// 	team.Name = newTeam.Name
// 	team.Description = newTeam.Description

// 	err = team.Save()
// 	if err != nil {
// 		log.Errorf("Error saving Team %v", team)
// 		return nil, err
// 	}

// 	return team, nil
// }

// func (s *teamService) DeleteTeam(org *schema.Organization, team *schema.Team) error {

// 	// we should not be able to delete Owners Team
// 	if org.OwnersTeamId == team.Id {
// 		return ErrForbiddenToDeleteOwnersTeam
// 	}

// 	// we shouldn't delete a team if it has members
// 	// so we try to retrieve all members that belongs to the team first
// 	members, err := models.Member.FindByTeam(team)
// 	if err != nil {
// 		log.Errorf("Error retrieving members for Team %v", team)
// 		return err
// 	}

// 	// if we found any members, then we bail with an error
// 	if len(members) > 0 {
// 		return ErrTeamHasMembers
// 	}

// 	err = models.Team.RemoveID(team.Id)
// 	if err != nil {
// 		log.Errorf("Error deleting Team %v", team)
// 		return err
// 	}
// 	return nil
// }

// func (s *teamService) getOrganizationFromTeam(team *schema.Team) (*schema.Organization, error) {
// 	org := &schema.Organization{}
// 	err := models.Organization.FindId(team.OrganizationId).One(org)
// 	if err != nil {
// 		log.Errorf("Failed to retrieve organization (%s) from database: %v ", team.OrganizationId.Hex(), err)
// 		return nil, err
// 	}
// 	return org, nil
// }

// func (s *teamService) getExistingTeam(org *schema.Organization, name string) (*schema.Team, error) {
// 	existingTeam, err := models.Team.FindByOrganizationIdAndName(org.Id, name)
// 	if err != nil && err != storage.ErrNotFound {
// 		log.Errorf("Failed to find team by organization_id and name: %v", err)
// 		return nil, err
// 	}
// 	return existingTeam, nil
// }

// func (s *teamService) checkOrganizationOwnership(user *schema.User, org *schema.Organization) error {

// 	// check if the current user is part of the organization owners
// 	ok, err := models.Member.IsOrganizationOwner(org, user)
// 	if err != nil {
// 		log.Errorf("Error checking if user (%s) is an organization owner (%s): %v ", user.Id.Hex(), org.Id.Hex(), err)
// 		return err
// 	}

// 	if !ok {
// 		log.Warnf("Unauthorized user (%s) for organization (%s) ", user.Id.Hex(), org.Id.Hex())
// 		return ErrForbiddenNotOrganizationOwner
// 	}

// 	return nil
// }

// func (s *teamService) checkOrganizationMembership(user *schema.User, org *schema.Organization) error {

// 	// check if the current user is part of the organization owners
// 	ok, err := models.Member.IsOrganizationMember(org, user)
// 	if err != nil {
// 		log.Errorf("Error checking if user (%s) is an organization member (%s): %v ", user.Id.Hex(), org.Id.Hex(), err)
// 		return err
// 	}

// 	if !ok {
// 		log.Warnf("Unauthorized user (%s) for organization (%s) ", user.Id.Hex(), org.Id.Hex())
// 		return ErrForbiddenNotOrganizationMember
// 	}

// 	return nil
// }
