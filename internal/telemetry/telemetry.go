package telemetry

import (
	"os"
	"strings"
	"sync"

	"github.com/bubustack/bobrapet/pkg/contracts"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var (
	initOnce                sync.Once
	tracePropagationEnabled = true
)

// InitFromEnv configures OTEL propagators according to cluster telemetry settings.
func InitFromEnv() {
	initOnce.Do(func() {
		tracePropagationEnabled = parseBoolEnv(contracts.TracePropagationEnv, true)
		if tracePropagationEnabled {
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
				propagation.TraceContext{},
				propagation.Baggage{},
			))
			return
		}
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
}

// TracePropagationEnabled reports whether gRPC stats handlers should be installed.
func TracePropagationEnabled() bool {
	initOnce.Do(func() {
		tracePropagationEnabled = true
	})
	return tracePropagationEnabled
}

func parseBoolEnv(key string, def bool) bool {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return def
	}
}
