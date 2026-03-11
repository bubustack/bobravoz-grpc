package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bubustack/bobravoz-grpc/internal/connector"
	"github.com/bubustack/bobravoz-grpc/internal/telemetry"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	opts := zap.Options{
		Development: false,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	log.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	logger := log.Log.WithName("connector")

	if err := telemetry.InitFromEnv("bobravoz-connector"); err != nil {
		logger.Error(err, "failed to initialize OTEL tracer provider")
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShutdown()
		if err := telemetry.Shutdown(shutdownCtx); err != nil {
			logger.Error(err, "failed to shutdown OTEL tracer provider")
		}
	}()

	cfg, err := connector.LoadConfigFromEnv()
	if err != nil {
		logger.Error(err, "failed to load connector configuration")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	runner := connector.NewRunner(cfg, logger)
	if err := runner.Run(ctx); err != nil && err != context.Canceled {
		logger.Error(err, "connector exited with error")
		os.Exit(1)
	}
	logger.Info("connector stopped gracefully")
}
