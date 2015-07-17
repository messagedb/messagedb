package retention_test

import (
	"testing"
	"time"

	"github.com/messagedb/messagedb/services/retention"

	"github.com/BurntSushi/toml"
)

func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	c := retention.NewConfig()

	if _, err := toml.Decode(`
enabled = true
check-interval = "1s"
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Enabled != true {
		t.Fatalf("unexpected enabled state: %v", c.Enabled)
	} else if time.Duration(c.CheckInterval) != time.Second {
		t.Fatalf("unexpected check interval: %v", c.CheckInterval)
	}
}
