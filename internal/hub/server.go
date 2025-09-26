package hub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	bobrapetcel "github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobravoz-grpc/proto"
	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	metaStoryRunName  = "storyrun-name"
	metaStoryRunNS    = "storyrun-namespace"
	metaCurrentStepID = "current-step-id"
)

// Server is the gRPC hub server.
type Server struct {
	proto.UnimplementedHubServer
	client        client.Client
	log           logr.Logger
	streamManager *StreamManager
}

// NewServer creates a new hub server.
func NewServer(client client.Client) *Server {
	return &Server{
		client:        client,
		log:           log.Log.WithName("hub-server"),
		streamManager: NewStreamManager(),
	}
}

// Start starts the gRPC server.
func (s *Server) Start(ctx context.Context, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterHubServer(grpcServer, s)

	s.log.Info("Starting gRPC hub server", "port", port)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			s.log.Error(err, "gRPC server failed")
		}
	}()

	// Listen for context cancellation to gracefully stop the server
	<-ctx.Done()
	s.log.Info("Shutting down gRPC hub server")
	grpcServer.GracefulStop()
	return nil
}

// Process is the bidirectional streaming RPC for the hub.
func (s *Server) Process(stream proto.Hub_ProcessServer) error {
	s.log.Info("New stream established")
	ctx := stream.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return errors.New("missing metadata")
	}

	storyRunName, storyRunNS, currentStepID, err := s.extractMetadata(md)
	if err != nil {
		s.log.Error(err, "Failed to extract metadata")
		return err
	}

	s.streamManager.AddStream(storyRunName, currentStepID, stream)
	defer s.streamManager.RemoveStream(storyRunName, currentStepID)

	// Start a separate goroutine to handle incoming messages from this client.
	go s.messageLoop(stream, storyRunName, storyRunNS, currentStepID)

	// Keep the stream alive, but the actual forwarding will be done by the upstream engram's goroutine.
	<-ctx.Done()
	s.log.Info("Stream context done", "storyRun", storyRunName, "step", currentStepID)
	return nil
}

func (s *Server) messageLoop(stream proto.Hub_ProcessServer, storyRunName, storyRunNS, currentStepID string) {
	defer s.streamManager.RemoveStream(storyRunName, currentStepID)

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			s.log.Info("Upstream closed the stream", "storyRun", storyRunName, "step", currentStepID)
			return
		}
		if err != nil {
			s.log.Error(err, "Error receiving from stream", "storyRun", storyRunName, "step", currentStepID)
			return
		}

		s.log.Info("Received packet", "storyRun", storyRunName, "fromStep", currentStepID)

		// Get the StoryRun and Story to find the next step.
		storyRun, story, err := s.getStoryAndRun(stream.Context(), storyRunName, storyRunNS)
		if err != nil {
			s.log.Error(err, "Failed to get story and run", "storyRun", storyRunName)
			continue // Or maybe we should terminate the stream?
		}

		nextStep, nextEngramStep, err := findNextSteps(story, currentStepID)
		if err != nil {
			s.log.Error(err, "Could not determine next step", "storyRun", storyRunName, "currentStep", currentStepID)
			continue
		}

		// This is where we will handle primitive steps.
		processedPayload := in.Payload
		if nextStep.Ref == nil { // It's a primitive
			var err error
			processedPayload, err = s.evaluatePrimitive(nextStep, in.Payload)
			if err != nil {
				s.log.Error(err, "Failed to evaluate primitive", "step", getStepID(nextStep))
				continue
			}
		}

		// If there's no subsequent engram, we're done with this branch of the pipeline.
		if nextEngramStep == nil {
			s.log.Info("End of engram chain for this packet.", "storyRun", storyRunName, "lastStep", currentStepID)
			continue
		}

		// Find the stream for the next engram step and forward the packet.
		nextEngramStepID := getStepID(nextEngramStep)
		if downstream, ok := s.streamManager.GetStream(storyRun.Name, nextEngramStepID); ok {
			out := &proto.DataPacket{
				Metadata: in.Metadata,
				Payload:  processedPayload,
			}
			if err := downstream.Send(out); err != nil {
				s.log.Error(err, "Failed to forward packet to downstream", "downstreamStep", nextEngramStepID)
			}
		} else {
			s.log.Info("Downstream not yet available", "downstreamStep", nextEngramStepID)
			// TODO: We should probably buffer this packet. For now, we'll drop it.
		}
	}
}

func (s *Server) evaluatePrimitive(step *bubuv1alpha1.Step, payload *structpb.Struct) (*structpb.Struct, error) {
	// For now, we only support CEL transform.
	// A real implementation would have a switch statement for different primitive types.
	if step.Type != enums.StepTypeTransform {
		s.log.Info("Skipping unsupported primitive", "type", step.Type)
		return payload, nil
	}
	if step.With == nil {
		return nil, errors.New("transform step is missing 'with' block")
	}

	var withMap map[string]interface{}
	if err := json.Unmarshal(step.With.Raw, &withMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal 'with' block: %w", err)
	}

	// The input for the CEL expression will be the payload of the data packet.
	inputVars := map[string]interface{}{
		"payload": payload.AsMap(),
	}

	evaluator, err := bobrapetcel.New(logging.NewCELLogger(s.log))
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL evaluator: %w", err)
	}

	resultMap, err := evaluator.ResolveWithInputs(withMap, inputVars)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate 'with' block: %w", err)
	}

	return structpb.NewStruct(resultMap)
}

func (s *Server) extractMetadata(md metadata.MD) (storyRunName, storyRunNS, currentStepID string, err error) {
	if len(md[metaStoryRunName]) == 0 || md[metaStoryRunName][0] == "" {
		return "", "", "", errors.New("missing storyrun-name metadata")
	}
	if len(md[metaStoryRunNS]) == 0 || md[metaStoryRunNS][0] == "" {
		return "", "", "", errors.New("missing storyrun-namespace metadata")
	}
	if len(md[metaCurrentStepID]) == 0 || md[metaCurrentStepID][0] == "" {
		return "", "", "", errors.New("missing current-step-id metadata")
	}
	return md[metaStoryRunName][0], md[metaStoryRunNS][0], md[metaCurrentStepID][0], nil
}

func (s *Server) getStoryAndRun(ctx context.Context, storyRunName, storyRunNS string) (*runsv1alpha1.StoryRun, *bubuv1alpha1.Story, error) {
	var storyRun runsv1alpha1.StoryRun
	if err := s.client.Get(ctx, types.NamespacedName{Name: storyRunName, Namespace: storyRunNS}, &storyRun); err != nil {
		return nil, nil, err
	}

	var story bubuv1alpha1.Story
	if err := s.client.Get(ctx, types.NamespacedName{Name: storyRun.Spec.StoryRef.Name, Namespace: storyRunNS}, &story); err != nil {
		return nil, nil, err
	}

	return &storyRun, &story, nil
}

func findNextSteps(story *bubuv1alpha1.Story, currentStepID string) (nextStep, nextEngramStep *bubuv1alpha1.Step, err error) {
	startIndex := -1
	for i, step := range story.Spec.Steps {
		if getStepID(&step) == currentStepID {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		return nil, nil, fmt.Errorf("step %q not found in story", currentStepID)
	}

	if startIndex+1 >= len(story.Spec.Steps) {
		return nil, nil, errors.New("current step is the last step")
	}

	nextStep = &story.Spec.Steps[startIndex+1]

	// Find the next engram step
	for i := startIndex + 1; i < len(story.Spec.Steps); i++ {
		if story.Spec.Steps[i].Ref != nil {
			nextEngramStep = &story.Spec.Steps[i]
			return
		}
	}

	return
}

func getStepID(step *bubuv1alpha1.Step) string {
	if step.ID != "" {
		return step.ID
	}
	return step.Name
}
