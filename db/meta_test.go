package db

import (
	"fmt"
	"testing"
)

func BenchmarkCreateConversationIndex_1K(b *testing.B) {
	benchmarkCreateConversationIndex(b, genTestConversations(38, 3, 3))
}

func BenchmarkCreateConversationIndex_100K(b *testing.B) {
	benchmarkCreateConversationIndex(b, genTestConversations(32, 5, 5))
}

func BenchmarkCreateConversationIndex_1M(b *testing.B) {
	benchmarkCreateConversationIndex(b, genTestConversations(330, 5, 5))
}

func benchmarkCreateConversationIndex(b *testing.B, conversations []*TestConversation) {
	idxs := make([]*DatabaseIndex, 0, b.N)
	for i := 0; i < b.N; i++ {
		idxs = append(idxs, NewDatabaseIndex())
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		idx := idxs[n]
		for _, s := range conversations {
			idx.createConversationIndexIfNotExists(s.Name, s.Conversation)
		}
	}
}

type TestConversation struct {
	Name         string
	Conversation *Conversation
}

func genTestConversations(mCnt, tCnt, vCnt int) []*TestConversation {
	names := genStrList("conversation", mCnt)
	tagSets := NewTagSetGenerator(tCnt, vCnt).AllSets()
	conversations := []*TestConversation{}
	for _, m := range names {
		for _, ts := range tagSets {
			conversations = append(conversations, &TestConversation{
				Name: m,
				Conversation: &Conversation{
					Key:  fmt.Sprintf("%s:%s", m, string(marshalTags(ts))),
					Tags: ts,
				},
			})
		}
	}
	return conversations
}

type TagValGenerator struct {
	Key  string
	Vals []string
	idx  int
}

func NewTagValGenerator(tagKey string, nVals int) *TagValGenerator {
	tvg := &TagValGenerator{Key: tagKey}
	for i := 0; i < nVals; i++ {
		tvg.Vals = append(tvg.Vals, fmt.Sprintf("tagValue%d", i))
	}
	return tvg
}

func (tvg *TagValGenerator) First() string {
	tvg.idx = 0
	return tvg.Curr()
}

func (tvg *TagValGenerator) Curr() string {
	return tvg.Vals[tvg.idx]
}

func (tvg *TagValGenerator) Next() string {
	tvg.idx++
	if tvg.idx >= len(tvg.Vals) {
		tvg.idx--
		return ""
	}
	return tvg.Curr()
}

type TagSet map[string]string

type TagSetGenerator struct {
	TagVals []*TagValGenerator
}

func NewTagSetGenerator(nSets int, nTagVals ...int) *TagSetGenerator {
	tsg := &TagSetGenerator{}
	for i := 0; i < nSets; i++ {
		nVals := nTagVals[0]
		if i < len(nTagVals) {
			nVals = nTagVals[i]
		}
		tagKey := fmt.Sprintf("tagKey%d", i)
		tsg.TagVals = append(tsg.TagVals, NewTagValGenerator(tagKey, nVals))
	}
	return tsg
}

func (tsg *TagSetGenerator) First() TagSet {
	for _, tsv := range tsg.TagVals {
		tsv.First()
	}
	return tsg.Curr()
}

func (tsg *TagSetGenerator) Curr() TagSet {
	ts := TagSet{}
	for _, tvg := range tsg.TagVals {
		ts[tvg.Key] = tvg.Curr()
	}
	return ts
}

func (tsg *TagSetGenerator) Next() TagSet {
	val := ""
	for _, tsv := range tsg.TagVals {
		if val = tsv.Next(); val != "" {
			break
		} else {
			tsv.First()
		}
	}

	if val == "" {
		return nil
	}

	return tsg.Curr()
}

func (tsg *TagSetGenerator) AllSets() []TagSet {
	allSets := []TagSet{}
	for ts := tsg.First(); ts != nil; ts = tsg.Next() {
		allSets = append(allSets, ts)
	}
	return allSets
}

func genStrList(prefix string, n int) []string {
	lst := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lst = append(lst, fmt.Sprintf("%s%d", prefix, i))
	}
	return lst
}

func strref(s string) *string {
	return &s
}
