package retention_test

import (
	"log"
	"os"
	"testing"

	"github.com/messagedb/messagedb/services/retention"
)

func TestServiceConstructor(t *testing.T) {

	config := retention.NewConfig()

	s := retention.NewService(config)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
}

func TestServiceSettingLogger(t *testing.T) {

	config := retention.NewConfig()
	s := retention.NewService(config)

	logger := log.New(os.Stderr, "[retention] ", log.LstdFlags)
	s.SetLogger(logger)
	if s.Logger() != logger {
		t.Fatalf("unexpected logger state: %v", s.Logger())
	}

}
