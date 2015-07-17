package httpd

type Config struct {
	Enabled        bool   `toml:"enabled"`
	BindAddress    string `toml:"bind-address"`
	BaseApiURL     string `toml:"base-api-url"`
	MaxConnections int    `toml:"max-connections"`
	AuthEnabled    bool   `toml:"auth-enabled"`
	LogEnabled     bool   `toml:"log-enabled"`
	WriteTracing   bool   `toml:"write-tracing"`
	PprofEnabled   bool   `toml:"pprof-enabled"`
}

func NewConfig() Config {
	return Config{
		Enabled:        true,
		BindAddress:    ":8075",
		LogEnabled:     true,
		MaxConnections: 5000,
	}
}
