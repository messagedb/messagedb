package run_test

import (
	"testing"

	"github.com/messagedb/messagedb/cmd/messagedbd/run"

	"github.com/BurntSushi/toml"
)

// Ensure the configuration can be parsed.
func TestConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c run.Config
	if _, err := toml.Decode(`
[meta]
dir = "/tmp/meta"

[data]
dir = "/tmp/data"

[cluster]

[admin]
bind-address = ":8088"

[http]
bind-address = ":8075"

`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.Meta.Dir != "/tmp/meta" {
		t.Fatalf("unexpected meta dir: %s", c.Meta.Dir)
	} else if c.Data.Dir != "/tmp/data" {
		t.Fatalf("unexpected data dir: %s", c.Data.Dir)
	} else if c.Admin.BindAddress != ":8088" {
		t.Fatalf("unexpected admin bind address: %s", c.Admin.BindAddress)
	} else if c.HTTPD.BindAddress != ":8075" {
		t.Fatalf("unexpected api bind address: %s", c.HTTPD.BindAddress)
	}
}
