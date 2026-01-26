package connector

import connectorcfg "github.com/bubustack/core/runtime/transport/connector"

// Config is the shared connector runtime configuration.
type Config = connectorcfg.Config

// LoadConfigFromEnv builds Config from the ambient environment.
func LoadConfigFromEnv() (*Config, error) {
	return connectorcfg.LoadConfigFromEnv(connectorcfg.OSEnv)
}
