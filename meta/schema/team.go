package schema

// import (
// 	"time"
// 	"gopkg.in/mgo.v2/bson"
// )

// type TeamType int

// const (
// 	TeamTypeOwners TeamType = iota
// 	TeamTypeAdmins
// 	TeamTypeTeams
// )

// type Team struct {
// 	storage.Document `bson:"-"`
// 	Id               bson.ObjectId `json:"id" bson:"_id,omitempty"`
// 	OrganizationId   bson.ObjectId `json:"org_id" bson:"org_id"`
// 	Name             string        `json:"name" bson:"name"`
// 	Description      string        `json:"description" bson:"description"`
// 	TeamType         TeamType      `json:"team_type" bson:"team_type"`
// 	CreatedAt        time.Time     `json:"created_at" bson:"created_at"`
// 	UpdatedAt        time.Time     `json:"updated_at" bson:"updated_at"`
// 	Errors           Errors        `json:"-" bson:"-"`
// }

// func (t *Team) CanBeDeleted() bool {
// 	return !(t.TeamType == TeamTypeOwners || t.TeamType == TeamTypeAdmins)
// }

// type Teams []Team

// func (ts Teams) Add(team Team) {
// 	for _, item := range ts {
// 		if item.Id == team.Id {
// 			return //found already, so bail
// 		}
// 	}
// 	ts = append(ts, team)
// }

// func (ts Teams) Remove(team Team) {
// 	for i, item := range ts {
// 		if item.Id == team.Id {
// 			// we found, so remove it
// 			ts = append(ts[:i], ts[i+1:]...)
// 			break
// 		}
// 	}

// }
