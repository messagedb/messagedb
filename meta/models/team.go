package models

// import (
// 	"github.com/messagedb/messagedb/meta/schema"
// 	"github.com/messagedb/messagedb/meta/utils"

// 	log "github.com/Sirupsen/logrus"
// 	"gopkg.in/mgo.v2"
// 	"gopkg.in/mgo.v2/bson"
// )

// var Team *TeamModel

// type TeamModel struct {
// 	*storage.Model
// }

// func (m *TeamModel) New() *schema.Team {
// 	return storage.CreateDocument(&schema.Team{}).(*schema.Team)
// }

// func (m *TeamModel) FindById(id interface{}) (*schema.Team, error) {
// 	team := &schema.Team{}
// 	err := m.FindId(id).One(team)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return team, nil
// }

// func (m *TeamModel) FindByOrganizationIdAndName(orgId interface{}, name string) (*schema.Team, error) {
// 	team := &schema.Team{}
// 	err := m.Find(bson.M{"org_id": utils.ObjectId(orgId), "name": name}).One(team)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return team, nil
// }

// func (m *TeamModel) FindOwnerTeamByOrganizationId(orgId interface{}) (*schema.Team, error) {
// 	return m.FindByOrganizationIdAndName(orgId, "owner")
// }

// func (m *TeamModel) FindAllByOrganizationId(orgId interface{}) ([]*schema.Team, error) {
// 	teams := []*schema.Team{}
// 	err := m.Find(bson.M{"org_id": utils.ObjectId(orgId)}).All(&teams)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return teams, nil
// }

// func init() {
// 	Team = storage.RegisterModel(schema.Team{}, "teams", func(col *mgo.Collection) interface{} {
// 		return &TeamModel{storage.NewModel(col)}
// 	}).(*TeamModel)

// 	// create required indexes in MongoDB
// 	indexes := []mgo.Index{}

// 	indexes = append(indexes, mgo.Index{
// 		Key:        []string{"org_id", "name"},
// 		Unique:     true,
// 		DropDups:   true,
// 		Background: true, // See notes.
// 		Sparse:     false,
// 	})

// 	for _, index := range indexes {
// 		err := Team.EnsureIndex(index)
// 		if err != nil {
// 			log.Panicf("Failed to ensure index on 'users' Collection: %v", index)
// 		}
// 	}

// }
