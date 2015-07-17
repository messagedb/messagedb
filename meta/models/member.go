package models

// import (
// 	"github.com/messagedb/messagedb/meta/schema"
// 	"github.com/messagedb/messagedb/meta/utils"
//
// 	"gopkg.in/mgo.v2/bson"
// )
//
// var Member *MemberModel
//
// type MemberModel struct {
// 	*storage.Model
// }
//
// func (m *MemberModel) New() *schema.Member {
// 	return storage.CreateDocument(&schema.Member{}).(*schema.Member)
// }
//
// func (m *MemberModel) FindById(id interface{}) (*schema.Member, error) {
// 	member := &schema.Member{}
// 	err := m.FindId(id).One(member)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return member, nil
// }
//
// func (m *MemberModel) FindByUserID(orgId interface{}, userId interface{}) (*schema.Member, error) {
// 	member := &schema.Member{}
// 	err := m.Find(bson.M{"org_id": utils.ObjectId(orgId), "user_id": utils.ObjectId(userId)}).One(member)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return member, nil
// }
//
// func (m *MemberModel) FindByInviteToken(orgId interface{}, token string) (*schema.Member, error) {
// 	member := &schema.Member{}
// 	err := m.Find(bson.M{"org_id": utils.ObjectId(orgId), "invite_token": token}).One(member)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return member, nil
// }
//
// func (m *MemberModel) FindAll(orgId interface{}) ([]*schema.Member, error) {
// 	members := []*schema.Member{}
// 	err := m.Find(bson.M{"org_id": utils.ObjectId(orgId)}).All(&members)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return members, nil
// }
//
// func (m *MemberModel) FindAllActive(orgId interface{}) ([]*schema.Member, error) {
// 	members := []*schema.Member{}
// 	err := m.Find(bson.M{"org_id": utils.ObjectId(orgId), "status": "active"}).All(&members)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return members, nil
// }
//
// func (m *MemberModel) FindAllPending(orgId interface{}) ([]*schema.Member, error) {
// 	members := []*schema.Member{}
// 	err := m.Find(bson.M{"org_id": utils.ObjectId(orgId), "status": "pending"}).All(&members)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return members, nil
// }

// func init() {
// 	Member = storage.RegisterModel(schema.Member{}, "members", func(col *mgo.Collection) interface{} {
// 		return &MemberModel{storage.NewModel(col)}
// 	}).(*MemberModel)
//
// 	// create required indexes in MongoDB
// 	indexes := []mgo.Index{}
//
// 	indexes = append(indexes, mgo.Index{
// 		Key:        []string{"user_id", "org_id"},
// 		Unique:     true,
// 		DropDups:   true,
// 		Background: true, // See notes.
// 		Sparse:     false,
// 	})
//
// 	for _, index := range indexes {
// 		err := Member.EnsureIndex(index)
// 		if err != nil {
// 			log.Panicf("Failed to ensure index on 'members' Collection: %v", index)
// 		}
// 	}
//
// }
