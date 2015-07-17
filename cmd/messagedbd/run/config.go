package run

import (
	"errors"
	"fmt"
	"os/user"
	"path/filepath"

	"github.com/messagedb/messagedb/cluster"
	"github.com/messagedb/messagedb/db"
	"github.com/messagedb/messagedb/meta"
	"github.com/messagedb/messagedb/services/admin"
	"github.com/messagedb/messagedb/services/hh"
	"github.com/messagedb/messagedb/services/httpd"
	"github.com/messagedb/messagedb/services/retention"
)

// Config represents the configuration format for the messaged binary.
type Config struct {
	Meta      meta.Config      `toml:"meta"`
	Data      db.Config        `toml:"data"`
	Cluster   cluster.Config   `toml:"cluster"`
	Retention retention.Config `toml:"retention"`

	Admin admin.Config `toml:"admin"`
	HTTPD httpd.Config `toml:"http"`

	HintedHandoff hh.Config `toml:"hinted-handoff"`

	// Server reporting
	ReportingDisabled bool `toml:"reporting-disabled"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {
	c := &Config{}
	c.Meta = meta.NewConfig()
	c.Data = db.NewConfig()
	c.Cluster = cluster.NewConfig()
	// c.Precreator = precreator.NewConfig()

	c.Admin = admin.NewConfig()
	c.HTTPD = httpd.NewConfig()
	// c.Collectd = collectd.NewConfig()
	// c.OpenTSDB = opentsdb.NewConfig()
	// c.Graphites = append(c.Graphites, graphite.NewConfig())

	// c.Monitoring = monitor.NewConfig()
	// c.ContinuousQuery = continuous_querier.NewConfig()
	c.Retention = retention.NewConfig()
	c.HintedHandoff = hh.NewConfig()

	return c
}

// NewDemoConfig returns the config that runs when no config is specified.
func NewDemoConfig() (*Config, error) {
	c := NewConfig()

	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to determine current user for storage")
	}

	c.Meta.Dir = filepath.Join(u.HomeDir, ".messagedb/meta")
	c.Data.Dir = filepath.Join(u.HomeDir, ".messagedb/data")
	c.HintedHandoff.Dir = filepath.Join(u.HomeDir, ".messagedb/hh")

	c.Admin.Enabled = true
	// c.Monitoring.Enabled = false

	return c, nil
}

// Validate returns an error if the config is invalid.
func (c *Config) Validate() error {
	if c.Meta.Dir == "" {
		return errors.New("Meta.Dir must be specified")
	} else if c.Data.Dir == "" {
		return errors.New("Data.Dir must be specified")
	} else if c.HintedHandoff.Dir == "" {
		return errors.New("HintedHandoff.Dir must be specified")
	}

	// for _, g := range c.Graphites {
	// 	if err := g.Validate(); err != nil {
	// 		return fmt.Errorf("invalid graphite config: %v", err)
	// 	}
	// }
	return nil
}
