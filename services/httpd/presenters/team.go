package presenters

// import (
// 	"fmt"
// 	"net/url"
// 	"time"

// 	"github.com/messagedb/messagedb/meta/schema"
// )

// type Team struct {
// 	Id             string          `json:"id"`
// 	OrganizationId string          `json:"org_id"`
// 	Name           string          `json:"name"`
// 	Description    string          `json:"description"`
// 	TeamType       schema.TeamType `json:"team_type"`
// 	CreatedAt      time.Time       `json:"created_at"`
// 	UpdatedAt      time.Time       `json:"updated_at"`
// }

// func (t *Team) GetLocation() *url.URL {
// 	uri, err := url.Parse(fmt.Sprintf("/teams/%s", t.Id))
// 	if err != nil {
// 		return nil
// 	}
// 	return uri
// }

// func TeamPresenter(t *schema.Team) *Team {
// 	team := &Team{}
// 	team.Id = t.Id.Hex()
// 	team.OrganizationId = t.OrganizationId.Hex()
// 	team.Name = t.Name
// 	team.Description = t.Description
// 	team.TeamType = t.TeamType
// 	team.CreatedAt = t.CreatedAt
// 	team.UpdatedAt = t.UpdatedAt

// 	return team
// }

// func TeamCollectionPresenter(items []*schema.Team) []*Team {
// 	collection := make([]*Team, 0)
// 	for _, item := range items {
// 		collection = append(collection, TeamPresenter(item))
// 	}
// 	return collection
// }
