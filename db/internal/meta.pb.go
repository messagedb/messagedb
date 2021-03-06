// Code generated by protoc-gen-gogo.
// source: internal/meta.proto
// DO NOT EDIT!

/*
Package internal is a generated protocol buffer package.

It is generated from these files:
	internal/meta.proto

It has these top-level messages:
	Conversation
	Tag
	MeasurementFields
	Field
*/
package internal

import proto "github.com/gogo/protobuf/proto"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

type Conversation struct {
	Key              *string `protobuf:"bytes,1,req" json:"Key,omitempty"`
	Tags             []*Tag  `protobuf:"bytes,2,rep" json:"Tags,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Conversation) Reset()         { *m = Conversation{} }
func (m *Conversation) String() string { return proto.CompactTextString(m) }
func (*Conversation) ProtoMessage()    {}

func (m *Conversation) GetKey() string {
	if m != nil && m.Key != nil {
		return *m.Key
	}
	return ""
}

func (m *Conversation) GetTags() []*Tag {
	if m != nil {
		return m.Tags
	}
	return nil
}

type Tag struct {
	Key              *string `protobuf:"bytes,1,req" json:"Key,omitempty"`
	Value            *string `protobuf:"bytes,2,req" json:"Value,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Tag) Reset()         { *m = Tag{} }
func (m *Tag) String() string { return proto.CompactTextString(m) }
func (*Tag) ProtoMessage()    {}

func (m *Tag) GetKey() string {
	if m != nil && m.Key != nil {
		return *m.Key
	}
	return ""
}

func (m *Tag) GetValue() string {
	if m != nil && m.Value != nil {
		return *m.Value
	}
	return ""
}

type MeasurementFields struct {
	Fields           []*Field `protobuf:"bytes,1,rep" json:"Fields,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *MeasurementFields) Reset()         { *m = MeasurementFields{} }
func (m *MeasurementFields) String() string { return proto.CompactTextString(m) }
func (*MeasurementFields) ProtoMessage()    {}

func (m *MeasurementFields) GetFields() []*Field {
	if m != nil {
		return m.Fields
	}
	return nil
}

type Field struct {
	ID               *int32  `protobuf:"varint,1,req" json:"ID,omitempty"`
	Name             *string `protobuf:"bytes,2,req" json:"Name,omitempty"`
	Type             *int32  `protobuf:"varint,3,req" json:"Type,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Field) Reset()         { *m = Field{} }
func (m *Field) String() string { return proto.CompactTextString(m) }
func (*Field) ProtoMessage()    {}

func (m *Field) GetID() int32 {
	if m != nil && m.ID != nil {
		return *m.ID
	}
	return 0
}

func (m *Field) GetName() string {
	if m != nil && m.Name != nil {
		return *m.Name
	}
	return ""
}

func (m *Field) GetType() int32 {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return 0
}

func init() {
}
