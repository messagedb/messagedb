package db

import (
	"fmt"
	"hash/fnv"
	"time"
)

type Message interface {
	Time() time.Time
	SetTime(t time.Time)
	UnixNano() int64

	HashID() uint64
	Key() []byte

	Data() []byte
	SetData(buf []byte)

	String() string
}

// message is the default implementation of Message.
type message struct {
	time time.Time

	key []byte

	// text encoding of timestamp
	ts []byte

	// binary encoded field data
	data []byte
}

func ParseMessagesString(buf string) ([]Message, error) {
	return ParseMessages([]byte(buf))
}

func ParseMessages(buf []byte) ([]Message, error) {
	messages := []Message{}
	var (
		pos   int
		block []byte
	)
	for {
		pos, block = scanTo(buf, pos, '\n')
		pos++

		if len(block) == 0 {
			break
		}

		if start := skipWhitespace(block, 0); block[start] == '#' {
			continue
		}

		m, err := parseMessage(buf, time.Now().UTC())
		if err != nil {
			return nil, fmt.Errorf("unable to parse '%s': %v", string(block), err)
		}
		messages = append(messages, m)

		if pos >= len(buf) {
			break
		}
	}

	return messages, nil
}

func parseMessage(buf []byte, defaultTime time.Time) (Message, error) {
	return nil, nil
}

func NewMessage(time time.Time) Message {
	//TODO: fix this here
	return &message{time: time}
}

func (m *message) Data() []byte {
	return m.data
}
func (m *message) SetData(b []byte) {
	m.data = b
}
func (m *message) Key() []byte {
	return m.key
}

func (m *message) Time() time.Time {
	return m.time
}

func (m *message) SetTime(t time.Time) {
	m.time = t
}

func (m *message) UnixNano() int64 {
	return m.Time().UnixNano()
}

func (m *message) HashID() uint64 {
	h := fnv.New64a()
	h.Write(m.key)
	sum := h.Sum64()
	return sum
}

func (m *message) String() string {
	return fmt.Sprintf("%s %d", m.Key(), m.UnixNano())
}

// skipWhitespace returns the end position within buf, starting at i after
// scanning over spaces in tags
func skipWhitespace(buf []byte, i int) int {
	for {
		if i >= len(buf) {
			return i
		}

		if buf[i] == '\\' {
			i += 2
			continue
		}
		if buf[i] == ' ' || buf[i] == '\t' {
			i++
			continue
		}
		break
	}
	return i
}

// scanTo returns the end position in buf and the next consecutive block
// of bytes, starting from i and ending with stop byte.  If there are leading
// spaces or escaped chars, they are skipped.
func scanTo(buf []byte, i int, stop byte) (int, []byte) {
	start := i
	for {
		// reached the end of buf?
		if i >= len(buf) {
			break
		}

		if buf[i] == '\\' {
			i += 2
			continue
		}

		// reached end of block?
		if buf[i] == stop {
			break
		}
		i++
	}

	return i, buf[start:i]
}
