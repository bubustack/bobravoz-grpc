package telemetry

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/bubustack/core/contracts"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.28.0"
)

var (
	initOnce                sync.Once
	shutdownOnce            sync.Once
	tracePropagationEnabled = true
	traceShutdown           func(context.Context) error
	initErr                 error
)

// InitFromEnv configures OTEL propagators according to cluster telemetry settings.
func InitFromEnv(serviceName string) error {
	initOnce.Do(func() {
		tracePropagationEnabled = parseBoolEnv(contracts.TracePropagationEnv, true)
		if tracePropagationEnabled {
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
				propagation.TraceContext{},
				propagation.Baggage{},
			))
		} else {
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
		}

		if !shouldEnableTracingExport() {
			return
		}

		exporter, err := otlptracegrpc.New(context.Background())
		if err != nil {
			initErr = err
			return
		}
		res, err := buildResource(context.Background(), serviceName)
		if err != nil {
			initErr = err
			return
		}
		provider := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(provider)
		traceShutdown = provider.Shutdown
	})
	return initErr
}

// TracePropagationEnabled reports whether gRPC stats handlers should be installed.
// InitFromEnv must be called before this function; otherwise the default is false.
func TracePropagationEnabled() bool {
	return tracePropagationEnabled
}

// Shutdown flushes and stops the tracer provider when initialized.
func Shutdown(ctx context.Context) error {
	if traceShutdown == nil {
		return nil
	}
	var err error
	shutdownOnce.Do(func() {
		err = traceShutdown(ctx)
	})
	return err
}

func shouldEnableTracingExport() bool {
	val := strings.TrimSpace(strings.ToLower(os.Getenv("OTEL_TRACES_EXPORTER")))
	if val == "none" {
		return false
	}
	if val != "" {
		return true
	}
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint != "" {
		return true
	}
	endpoint = strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
	return endpoint != ""
}

func buildResource(ctx context.Context, serviceName string) (*resource.Resource, error) {
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return res, err
	}
	if serviceName == "" || hasServiceName(res) {
		return res, err
	}
	fallback := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String(serviceName))
	merged, mergeErr := resource.Merge(res, fallback)
	if mergeErr != nil {
		return res, mergeErr
	}
	return merged, err
}

func hasServiceName(res *resource.Resource) bool {
	if res == nil {
		return false
	}
	iter := res.Iter()
	for iter.Next() {
		kv := iter.Attribute()
		if kv.Key == semconv.ServiceNameKey {
			return kv.Value.AsString() != ""
		}
	}
	return false
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
