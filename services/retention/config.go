package retention

import (
	"time"

	"github.com/messagedb/messagedb/toml"
)

type Config struct {
	Enabled       bool          `toml:"enabled"`
	CheckInterval toml.Duration `toml:"check-interval"`
}

func NewConfig() Config {
	return Config{Enabled: true, CheckInterval: toml.Duration(10 * time.Minute)}
}
