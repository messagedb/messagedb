package models

// import (
// 	"github.com/messagedb/messagedb/meta/schema"
// )
//
// // Device is the singleton instance for Device model
// var Device *DeviceModel
//
// // func init() {
// // 	Device = storage.RegisterModel(schema.Device{}, "device", func(col *mgo.Collection) interface{} {
// // 		return &DeviceModel{storage.NewModel(col)}
// // 	}).(*DeviceModel)
// // }
//
// // DeviceModel represent the model class for Device collection
// type DeviceModel struct {
// 	*storage.Model
// }
//
// // New creates a new instance of the DeviceModel struct
// func (m *DeviceModel) New() *schema.Device {
// 	return storage.CreateDocument(&schema.Device{}).(*schema.Device)
// }
//
// // FindByID returns the device record by ID
// func (m *DeviceModel) FindByID(id string) (*schema.Device, error) {
// 	device := &schema.Device{}
// 	err := m.FindId(id).One(device)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return device, nil
// }
