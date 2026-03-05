package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

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

	telemetry.InitFromEnv()

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
