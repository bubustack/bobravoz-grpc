package transport

import (
	"context"
	"fmt"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// AnnotationTransport is the annotation key used to specify the transport type.
	AnnotationTransport = "bobravoz.bubustack.io/transport"
	// GRPCTransportType is the identifier for the gRPC transport.
	GRPCTransportType = "grpc"
)

// Transport is the interface for managing network connectivity for a StoryRun.
type Transport interface {
	// Reconcile ensures the transport is correctly configured for the given StoryRun.
	Reconcile(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) error
	// EnsureCleanUp performs any necessary cleanup when a StoryRun is deleted.
	EnsureCleanUp(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) error
}

// Builder is a function that creates a new Transport.
type Builder func(client.Client) (Transport, error)

var transportBuilders = make(map[string]Builder)

// Register adds a new transport builder to the registry.
func Register(name string, builder Builder) {
	if _, exists := transportBuilders[name]; exists {
		panic(fmt.Sprintf("transport builder already registered: %s", name))
	}
	transportBuilders[name] = builder
}

// Get returns a new Transport instance for the given name.
func Get(name string, cli client.Client) (Transport, error) {
	builder, exists := transportBuilders[name]
	if !exists {
		return nil, fmt.Errorf("no transport builder registered for %q", name)
	}
	return builder(cli)
}

func init() {
	Register(GRPCTransportType, func(cli client.Client) (Transport, error) {
		return NewGRPCTransport(cli), nil
	})
}
