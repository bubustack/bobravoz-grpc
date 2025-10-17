package connector

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/bubustack/bobrapet/pkg/contracts"
)

const defaultLocalHost = "127.0.0.1"

// Config captures runtime settings for the transport connector sidecar.
type Config struct {
	LocalEndpoint            string
	LocalServerName          string
	HubEndpoint              string
	StoryRunName             string
	Namespace                string
	StepID                   string
	AllowInsecureHub         bool
	LocalDialTimeout         time.Duration
	HubDialTimeout           time.Duration
	Binding                  bindingReference
	BindingHeartbeatInterval time.Duration
}

// LoadConfigFromEnv builds a Config from standard connector environment variables.
func LoadConfigFromEnv() (*Config, error) {
	cfg := &Config{
		LocalEndpoint:    resolveLocalEndpoint(),
		HubEndpoint:      strings.TrimSpace(os.Getenv(contracts.HubEndpointEnv)),
		StoryRunName:     strings.TrimSpace(os.Getenv(contracts.StoryRunIDEnv)),
		Namespace:        resolveNamespace(),
		StepID:           strings.TrimSpace(os.Getenv(contracts.StepNameEnv)),
		AllowInsecureHub: resolveAllowInsecureHub(),
		LocalDialTimeout: parseDurationWithDefault(os.Getenv(contracts.GRPCDialTimeoutEnv), 5*time.Second),
		HubDialTimeout:   parseDurationWithDefault(os.Getenv(contracts.GRPCDialTimeoutEnv), 10*time.Second),
		BindingHeartbeatInterval: parseDurationWithDefault(
			os.Getenv(contracts.TransportHeartbeatIntervalEnv),
			30*time.Second,
		),
	}

	if cfg.HubEndpoint == "" {
		return nil, fmt.Errorf("%s must be set", contracts.HubEndpointEnv)
	}
	if cfg.StoryRunName == "" {
		return nil, fmt.Errorf("%s must be set", contracts.StoryRunIDEnv)
	}
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("%s or POD_NAMESPACE must be set", contracts.PodNamespaceEnv)
	}
	if cfg.StepID == "" {
		return nil, fmt.Errorf("%s must be set", contracts.StepNameEnv)
	}
	bindingEnv := strings.TrimSpace(os.Getenv(contracts.TransportBindingEnv))
	if bindingEnv == "" {
		return nil, fmt.Errorf("%s must be set", contracts.TransportBindingEnv)
	}
	ref, err := parseBindingEnv(bindingEnv)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", contracts.TransportBindingEnv, err)
	}
	if ref.Namespace == "" {
		ref.Namespace = cfg.Namespace
	}
	cfg.Binding = ref

	if engramName := strings.TrimSpace(os.Getenv(contracts.EngramNameEnv)); engramName != "" && cfg.Namespace != "" {
		cfg.LocalServerName = fmt.Sprintf("%s.%s.svc.cluster.local", engramName, cfg.Namespace)
	}
	return cfg, nil
}

func resolveLocalEndpoint() string {
	port := os.Getenv(contracts.GRPCPortEnv)
	if strings.TrimSpace(port) == "" {
		port = "50051"
	}
	if strings.Contains(port, ":") {
		return port
	}
	return fmt.Sprintf("%s:%s", defaultLocalHost, port)
}

func resolveNamespace() string {
	if ns := strings.TrimSpace(os.Getenv(contracts.PodNamespaceEnv)); ns != "" {
		return ns
	}
	return strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
}

func parseDurationWithDefault(raw string, def time.Duration) time.Duration {
	if strings.TrimSpace(raw) == "" {
		return def
	}
	if dur, err := time.ParseDuration(raw); err == nil && dur > 0 {
		return dur
	}
	return def
}

func resolveAllowInsecureHub() bool {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv(contracts.TransportSecurityModeEnv)))
	return mode == contracts.TransportSecurityModePlaintext
}
