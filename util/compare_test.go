package util_test

import (
	"testing"

	"github.com/messagedb/messagedb/util"
)

func TestSize_SecureCompare_DifferentLength(t *testing.T) {
	r := util.SecureCompare("abc", "abcdef")
	if r {
		t.Fatalf("unexpected result:\n\nexp=%v\n\ngot=%v\n\n", false, r)
	}
}

func TestSize_SecureCompare_SameLength_Different(t *testing.T) {
	r := util.SecureCompare("abc", "xyz")
	if r {
		t.Fatalf("unexpected result:\nexp=%v\ngot=%v\n\n", false, r)
	}
}

func TestSize_SecureCompare_Equal(t *testing.T) {
	r := util.SecureCompare("abc", "abc")
	if !r {
		t.Fatalf("unexpected result:\n\nexp=%v\n\ngot=%v\n\n", false, r)
	}
}
