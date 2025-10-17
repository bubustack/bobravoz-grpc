/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hub

import (
	"context"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	bobrapetcel "github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/contracts"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bobravoz-grpc/internal/telemetry"
	grpc_metrics "github.com/bubustack/bobravoz-grpc/pkg/metrics"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	metaStoryRunName         = "storyrun-name"
	metaStoryRunNS           = "storyrun-namespace"
	metaCurrentStepID        = "current-step-id"
	trueString               = "true"
	defaultPerMessageTimeout = 10 * time.Minute

	DefaultHubTLSDir  = "/var/run/hub-tls"
	DefaultHubTLSCert = DefaultHubTLSDir + "/tls.crt"
	DefaultHubTLSKey  = DefaultHubTLSDir + "/tls.key"
	DefaultHubTLSCA   = DefaultHubTLSDir + "/ca.crt"
)

// Server is the gRPC hub server.
type Server struct {
	transportpb.UnimplementedHubServiceServer
	client            client.Client
	cache             *storyCache
	log               logr.Logger
	celEvaluator      *bobrapetcel.Evaluator
	streamManager     *StreamManager
	storageManager    *storage.StorageManager
	perMessageTimeout time.Duration
	channelBufferSize int
	closeOnce         sync.Once
}

// NewServer creates a new hub server.
func NewServer(ctx context.Context, k8sClient client.Client, celCfg bobrapetcel.Config) (*Server, error) {
	cache := newStoryCache(k8sClient)

	// Initialize storage with a timeout to prevent indefinite hangs on misconfigured S3/storage.
	// Storage init should complete quickly (credential validation, endpoint checks) or fail fast.
	// Production deployments must ensure storage credentials/endpoints are valid at deploy time.
	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	storageMgr, err := storage.NewManager(initCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage manager: %w", err)
	}
	logger := log.Log.WithName("hub-server")
	celLogger := logging.NewCELLogger(logger)
	celEvaluator, err := bobrapetcel.New(celLogger, celCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL evaluator: %w", err)
	}
	return &Server{
		client:            k8sClient,
		cache:             cache,
		log:               logger,
		celEvaluator:      celEvaluator,
		streamManager:     NewStreamManager(),
		storageManager:    storageMgr,
		channelBufferSize: getChannelBufferSize(),
	}, nil
}

// Start starts the gRPC server.
// The caller is responsible for calling Close() after Start() returns to release resources.
func (s *Server) Start(ctx context.Context, port int, allowInsecure bool) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	var opts []grpc.ServerOption
	if telemetry.TracePropagationEnabled() {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}
	opts = append(opts,
		grpc.ChainStreamInterceptor(
			grpc_prometheus.StreamServerInterceptor,
			grpc_metrics.StreamServerInterceptor(),
		),
		grpc.ChainUnaryInterceptor(
			grpc_prometheus.UnaryServerInterceptor,
			grpc_metrics.UnaryServerInterceptor(),
		),
	)

	if tlsOpt, err := tlsServerOptionFromEnv(s.log, allowInsecure); err != nil {
		return err
	} else if tlsOpt != nil {
		opts = append(opts, tlsOpt)
	}

	if kaOpt, ok := serverKeepaliveOptionFromEnv(); ok {
		opts = append(opts, kaOpt)
	}

	recvMax, sendMax := parseMaxMsgSizesFromEnv()
	opts = append(opts, grpc.MaxRecvMsgSize(recvMax), grpc.MaxSendMsgSize(sendMax))

	s.perMessageTimeout = parsePerMessageTimeoutFromEnv(s.log)

	grpcServer := grpc.NewServer(opts...)
	transportpb.RegisterHubServiceServer(grpcServer, s)

	// Enable latency histograms and register gRPC metrics server
	grpc_prometheus.EnableHandlingTimeHistogram()
	grpc_prometheus.Register(grpcServer)

	// Start background evictor to cap buffer cardinality over time
	s.streamManager.StartEvictor(ctx)

	// Start a heartbeat sender to keep downstream connections alive.
	s.startHeartbeatSender(ctx)

	s.log.Info("Starting gRPC hub server", "port", port)
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		s.log.Info("Shutting down gRPC hub server")
		grpcServer.GracefulStop()
		if err := <-serveErrCh; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.log.Error(err, "gRPC server shutdown returned error")
			return err
		}
		return nil
	case err := <-serveErrCh:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.log.Error(err, "gRPC server failed before shutdown")
			return err
		}
		return nil
	}
}

// Close releases long-lived resources owned by the Server.
func (s *Server) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		if s.celEvaluator != nil {
			s.celEvaluator.Close()
		}
	})
}

// isHeartbeat checks if a DataPacket is a heartbeat message.
func isHeartbeat(packet *transportpb.DataPacket) bool {
	return packet != nil && packet.Metadata != nil && packet.Metadata["bubu-heartbeat"] == trueString
}

func copyMetadataForStep(base map[string]string, storyRunName, storyRunNS, stepID string) map[string]string {
	newMD := make(map[string]string, len(base)+3)
	for k, v := range base {
		newMD[k] = v
	}
	if storyRunName != "" {
		newMD[metaStoryRunName] = storyRunName
	}
	if storyRunNS != "" {
		newMD[metaStoryRunNS] = storyRunNS
	}
	if stepID != "" {
		newMD[metaCurrentStepID] = stepID
	}
	return newMD
}

func cloneTransports(src []*transportpb.TransportDescriptor) []*transportpb.TransportDescriptor {
	if len(src) == 0 {
		return nil
	}
	out := make([]*transportpb.TransportDescriptor, len(src))
	for i, td := range src {
		if td == nil {
			continue
		}
		cloned := proto.Clone(td)
		desc, ok := cloned.(*transportpb.TransportDescriptor)
		if !ok {
			continue
		}
		out[i] = desc
	}
	return out
}

func cloneAudioFrame(src *transportpb.AudioFrame) *transportpb.AudioFrame {
	if src == nil {
		return nil
	}
	cloned, ok := proto.Clone(src).(*transportpb.AudioFrame)
	if !ok {
		return nil
	}
	cloned.Pcm = append([]byte(nil), src.GetPcm()...)
	return cloned
}

func cloneVideoFrame(src *transportpb.VideoFrame) *transportpb.VideoFrame {
	if src == nil {
		return nil
	}
	cloned, ok := proto.Clone(src).(*transportpb.VideoFrame)
	if !ok {
		return nil
	}
	cloned.Payload = append([]byte(nil), src.GetPayload()...)
	return cloned
}

func cloneBinaryFrame(src *transportpb.BinaryFrame) *transportpb.BinaryFrame {
	if src == nil {
		return nil
	}
	cloned, ok := proto.Clone(src).(*transportpb.BinaryFrame)
	if !ok {
		return nil
	}
	cloned.Payload = append([]byte(nil), src.GetPayload()...)
	return cloned
}

func (s *Server) startHeartbeatSender(ctx context.Context) {
	interval := 10 * time.Second // Default interval, matches SDK
	if v := os.Getenv(contracts.GRPCHeartbeatIntervalEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			interval = d
		}
	}

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		defer func() {
			if r := recover(); r != nil {
				s.log.Error(fmt.Errorf("heartbeat sender panic: %v", r), "Heartbeat sender crashed")
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.streamManager.SendHeartbeats(ctx)
			}
		}
	}()
}

// Process is the bidirectional streaming RPC for the hub.
func (s *Server) Process(stream transportpb.HubService_ProcessServer) error {
	s.log.Info("New stream established")
	ctx := stream.Context()
	// Apply a stream-wide deadline only when upstream didn't supply one.
	if s.perMessageTimeout > 0 {
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.perMessageTimeout)
			defer cancel()
		} else {
			s.log.V(1).Info("Honoring upstream stream deadline; skipping hub default")
		}
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		err := errors.New("missing metadata")
		s.log.Error(err, "Incoming hub stream missing metadata")
		return err
	}
	s.log.Info("Incoming stream metadata snapshot", "metadata", md)

	storyRunName, storyRunNS, currentStepID, err := s.extractMetadata(md)
	if err != nil {
		s.log.Error(err, "Failed to extract metadata")
		return err
	}
	s.log.Info("Hub stream metadata extracted",
		"storyRun", storyRunName,
		"namespace", storyRunNS,
		"step", currentStepID,
	)

	streamEntry := s.streamManager.AddStream(ctx, storyRunName, storyRunNS, currentStepID, stream)
	s.log.Info("Hub stream metadata",
		"storyRun", storyRunName,
		"namespace", storyRunNS,
		"step", currentStepID,
	)
	defer s.streamManager.RemoveStream(storyRunName, storyRunNS, currentStepID, streamEntry)

	// Handle incoming messages in this goroutine and return when the stream ends
	if err := s.messageLoop(ctx, stream, storyRunName, storyRunNS, currentStepID); err != nil {
		return err
	}
	s.log.Info("Stream ended", "storyRun", storyRunName, "step", currentStepID)
	return nil
}

// recvResult represents the result of a stream.Recv() operation
type recvResult struct {
	req *transportpb.ProcessRequest
	err error
}

func (s *Server) messageLoop(ctx context.Context, stream transportpb.HubService_ProcessServer, storyRunName, storyRunNS, currentStepID string) error {
	// Start a single receiver goroutine that feeds into recvCh
	// This prevents unbounded goroutine creation on each recv attempt
	recvCh := make(chan recvResult, 1)
	go func() {
		defer close(recvCh)
		for {
			req, err := stream.Recv()
			result := recvResult{req: req, err: err}
			select {
			case <-ctx.Done():
				// Context canceled; exit goroutine
				return
			case recvCh <- result:
				if err != nil {
					// EOF or other error; exit goroutine
					return
				}
			}
		}
	}()

	// Main message processing loop
	for {
		select {
		case <-ctx.Done():
			s.log.Info("Stream context done", "storyRun", storyRunName, "step", currentStepID, "err", ctx.Err())
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				s.log.Info("Stream deadline exceeded", "storyRun", storyRunName, "step", currentStepID)
				return status.Errorf(codes.DeadlineExceeded, "hub stream deadline exceeded: %v", err)
			}
			s.log.Info("Stream context done, closing message loop", "storyRun", storyRunName, "step", currentStepID)
			return err

		case result, ok := <-recvCh:
			if !ok {
				// Channel closed unexpectedly
				s.log.Info("recvCh closed unexpectedly", "storyRun", storyRunName, "step", currentStepID)
				return io.EOF
			}

			if result.err != nil {
				if result.err == io.EOF {
					s.log.Info("Upstream closed the stream", "storyRun", storyRunName, "step", currentStepID)
					return nil
				}
				s.log.Error(result.err, "Error receiving from stream", "storyRun", storyRunName, "step", currentStepID)
				return result.err
			}

			packet := result.req.GetPacket()
			if packet == nil {
				s.log.Info("Received empty packet wrapper", "storyRun", storyRunName, "step", currentStepID)
				continue // ignore empty wrapper
			}
			s.log.Info("Received packet from hub", "storyRun", storyRunName, "step", currentStepID, "metadataKeys", len(packet.Metadata))

			if !isHeartbeat(packet) {
				grpc_metrics.RecordHubMessageReceived(storyRunName, currentStepID)
			} else {
				s.log.V(1).Info("Heartbeat received", "storyRun", storyRunName, "step", currentStepID)
			}

			// Process the packet (may be slow due to K8s API calls)
			if err := s.processPacket(ctx, storyRunName, storyRunNS, currentStepID, packet); err != nil {
				return err
			}
		}
	}
}

// processPacket executes the routing logic for a received packet.
func (s *Server) processPacket(ctx context.Context, storyRunName, storyRunNS, currentStepID string, in *transportpb.DataPacket) error {
	storyRun, story, err := s.getStoryAndRunWithRetry(ctx, storyRunName, storyRunNS)
	if err != nil {
		s.log.Error(err, "Failed to get story and run after retries", "storyRun", storyRunName)
		return status.Errorf(codes.Unavailable, "failed to get story backend data: %v", err)
	}

	primitiveSteps, nextEngramStep, err := findNextSteps(story, currentStepID)
	if err != nil {
		if strings.Contains(err.Error(), "last step") {
			s.log.Info("End of pipeline reached for packet", "storyRun", storyRunName, "lastStep", currentStepID)
		} else {
			s.log.Error(err, "Could not determine next step", "storyRun", storyRunName, "currentStep", currentStepID)
		}
		return nil
	}

	storyInputs, inputsErr := rawExtensionToMap(storyRun.Spec.Inputs)
	if inputsErr != nil {
		s.log.Error(inputsErr, "Failed to parse story inputs; downstream CEL expressions may lack 'inputs'", "storyRun", storyRunName)
	}

	if s.handleHeartbeatPacket(ctx, storyRun, nextEngramStep, currentStepID, in) {
		return nil
	}

	s.log.Info("Received packet", "storyRun", storyRunName, "fromStep", currentStepID)

	processedPayload, stepVars, recorded, err := s.evaluatePrimitiveChain(ctx, storyRunName, currentStepID, primitiveSteps, in, storyInputs)
	if err != nil {
		return err
	}
	if !recorded {
		return nil
	}

	evaluatedInputs, deferred, err := s.evaluateNextEngramInputs(ctx, storyRunName, nextEngramStep, processedPayload, stepVars, storyInputs)
	if err != nil {
		return err
	}
	if deferred {
		return nil
	}

	return s.routePacket(ctx, storyRun, story, nextEngramStep, evaluatedInputs, currentStepID, processedPayload, in)
}

func (s *Server) handleHeartbeatPacket(ctx context.Context, storyRun *runsv1alpha1.StoryRun, nextEngramStep *bubuv1alpha1.Step, currentStepID string, in *transportpb.DataPacket) bool {
	if !isHeartbeat(in) {
		return false
	}
	s.log.V(1).Info("Forwarding heartbeat", "from", currentStepID, "storyRun", storyRun.Name)
	if nextEngramStep == nil {
		return true
	}
	nextEngramStepID := getStepID(nextEngramStep)
	heartbeatPacket := &transportpb.DataPacket{
		Metadata: copyMetadataForStep(in.Metadata, storyRun.Name, storyRun.Namespace, nextEngramStepID),
		Payload:  in.Payload,
		Audio:    cloneAudioFrame(in.GetAudio()),
		Video:    cloneVideoFrame(in.GetVideo()),
		Binary:   cloneBinaryFrame(in.GetBinary()),
	}
	if ok := s.streamManager.SendOrBuffer(ctx, storyRun.Name, storyRun.Namespace, nextEngramStepID, heartbeatPacket); !ok {
		s.log.Info("Failed to deliver or buffer heartbeat; dropping", "downstreamStep", nextEngramStepID, "reason", "buffer_full")
	}
	return true
}

func (s *Server) evaluatePrimitiveChain(
	ctx context.Context,
	storyRunName, currentStepID string,
	primitiveSteps []*bubuv1alpha1.Step,
	packet *transportpb.DataPacket,
	storyInputs map[string]any,
) (*structpb.Struct, map[string]any, bool, error) {
	stepVars, recorded := setStepOutputs(nil, currentStepID, packet.Payload, packet.Inputs)
	if !recorded {
		s.log.V(1).Info("Packet produced no outputs; skipping downstream evaluation",
			"storyRun", storyRunName,
			"step", currentStepID,
		)
		return packet.Payload, stepVars, false, nil
	}

	processedPayload := packet.Payload
	for _, primitiveStep := range primitiveSteps {
		if primitiveStep.Ref != nil {
			continue
		}

		var err error
		processedPayload, err = s.evaluatePrimitive(ctx, primitiveStep, processedPayload, stepVars, storyInputs)
		if err != nil {
			stepID := getStepID(primitiveStep)
			s.log.Error(err, "Failed to evaluate primitive", "step", stepID)
			return nil, nil, false, status.Errorf(codes.Internal, "failed to evaluate primitive step %q: %v", stepID, err)
		}

		var recordedPrimitive bool
		stepVars, recordedPrimitive = setStepOutputs(stepVars, getStepID(primitiveStep), processedPayload, nil)
		if !recordedPrimitive {
			s.log.V(1).Info("Primitive produced no outputs; continuing",
				"storyRun", storyRunName,
				"primitiveStep", getStepID(primitiveStep),
			)
		}
	}

	return processedPayload, stepVars, true, nil
}

func (s *Server) evaluateNextEngramInputs(
	ctx context.Context,
	storyRunName string,
	nextEngramStep *bubuv1alpha1.Step,
	payload *structpb.Struct,
	stepVars map[string]any,
	storyInputs map[string]any,
) (*structpb.Struct, bool, error) {
	if nextEngramStep == nil {
		return nil, false, nil
	}

	evaluatedInputs, err := s.evaluateEngramInputs(ctx, nextEngramStep, payload, stepVars, storyInputs)
	if err != nil {
		stepID := getStepID(nextEngramStep)
		var blocked *bobrapetcel.ErrEvaluationBlocked
		if errors.As(err, &blocked) {
			s.log.V(1).Info("Deferring packet; upstream outputs not ready",
				"storyRun", storyRunName,
				"step", stepID,
				"reason", blocked.Reason,
			)
			return nil, true, nil
		}
		s.log.Error(err, "Failed to evaluate engram inputs", "step", stepID)
		return nil, false, status.Errorf(codes.Internal, "failed to evaluate engram inputs for step %q: %v", stepID, err)
	}

	return evaluatedInputs, false, nil
}

func (s *Server) routePacket(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	nextEngramStep *bubuv1alpha1.Step,
	evaluatedInputs *structpb.Struct,
	currentStepID string,
	payload *structpb.Struct,
	originalPacket *transportpb.DataPacket,
) error {
	if nextEngramStep == nil {
		s.log.Info("End of engram chain for this packet.", "storyRun", storyRun.Name, "lastStep", currentStepID)
		return nil
	}

	nextEngram, err := s.resolveEngramForStep(ctx, storyRun.Namespace, nextEngramStep)
	if err != nil {
		return err
	}

	handled, err := s.handleBatchEngramIfNeeded(ctx, storyRun, story, nextEngramStep, nextEngram, evaluatedInputs, currentStepID)
	if handled || err != nil {
		return err
	}

	return s.forwardToRealtimeStep(ctx, storyRun, nextEngramStep, payload, evaluatedInputs, originalPacket)
}

func (s *Server) resolveEngramForStep(ctx context.Context, namespace string, nextEngramStep *bubuv1alpha1.Step) (*bubuv1alpha1.Engram, error) {
	if nextEngramStep == nil || nextEngramStep.Ref == nil {
		return nil, nil
	}

	var nextEngram bubuv1alpha1.Engram
	key := types.NamespacedName{Namespace: namespace, Name: nextEngramStep.Ref.Name}
	if err := s.client.Get(ctx, key, &nextEngram); err != nil {
		statusCode := codes.Unavailable
		if apierrors.IsNotFound(err) {
			statusCode = codes.NotFound
		}
		s.log.Error(err, "Failed to resolve downstream engram for packet", "step", getStepID(nextEngramStep))
		return nil, status.Errorf(statusCode, "failed to resolve downstream engram %q: %v", key.Name, err)
	}
	return &nextEngram, nil
}

func (s *Server) handleBatchEngramIfNeeded(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	step *bubuv1alpha1.Step,
	engram *bubuv1alpha1.Engram,
	evaluatedInputs *structpb.Struct,
	currentStepID string,
) (bool, error) {
	if engram == nil || engram.Spec.Mode != enums.WorkloadModeJob {
		return false, nil
	}

	if err := s.createBatchStepRun(ctx, storyRun, story, step, evaluatedInputs, currentStepID); err != nil {
		s.log.Error(err, "Failed to create StepRun for batch engram", "step", getStepID(step))
		return true, status.Errorf(codes.Internal, "failed to create StepRun for batch step: %v", err)
	}

	return true, nil
}

func (s *Server) forwardToRealtimeStep(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	nextEngramStep *bubuv1alpha1.Step,
	payload *structpb.Struct,
	evaluatedInputs *structpb.Struct,
	originalPacket *transportpb.DataPacket,
) error {
	nextEngramStepID := getStepID(nextEngramStep)
	out := &transportpb.DataPacket{
		Metadata:   copyMetadataForStep(originalPacket.Metadata, storyRun.Name, storyRun.Namespace, nextEngramStepID),
		Payload:    payload,
		Inputs:     evaluatedInputs,
		Transports: cloneTransports(originalPacket.GetTransports()),
		Audio:      cloneAudioFrame(originalPacket.GetAudio()),
		Video:      cloneVideoFrame(originalPacket.GetVideo()),
		Binary:     cloneBinaryFrame(originalPacket.GetBinary()),
	}
	if ok := s.streamManager.SendOrBuffer(ctx, storyRun.Name, storyRun.Namespace, nextEngramStepID, out); !ok {
		s.log.Info("Failed to deliver or buffer packet; dropping and closing stream", "storyRun", storyRun.Name, "downstreamStep", nextEngramStepID, "reason", "buffer_exhausted")
		return status.Errorf(codes.ResourceExhausted, "downstream buffer full for step %q", nextEngramStepID)
	}
	return nil
}

// getStoryAndRunWithRetry fetches StoryRun and Story with exponential backoff for transient errors.
// Uses context-aware sleep to respect cancellation and avoid thundering herd on API server.
func (s *Server) getStoryAndRunWithRetry(ctx context.Context, storyRunName, storyRunNS string) (*runsv1alpha1.StoryRun, *bubuv1alpha1.Story, error) {
	var (
		storyRun *runsv1alpha1.StoryRun
		story    *bubuv1alpha1.Story
		err      error
	)
	backoff := 50 * time.Millisecond
	for i := 0; i < 3; i++ {
		// Sleep with backoff before retry (except first attempt)
		if i > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, nil, ctx.Err()
			case <-timer.C:
			}
			backoff *= 2 // 50ms → 100ms → 200ms
		}

		storyRun, story, err = s.cache.Get(ctx, storyRunName, storyRunNS)
		if err == nil {
			return storyRun, story, nil
		}
		s.log.V(1).Info("Retrying to get story and run from cache", "storyRun", storyRunName, "attempt", i+1, "nextBackoff", backoff)
	}
	return nil, nil, err
}

// generateStepRunName composes a DNS-1123 compliant StepRun name that stays within Kubernetes limits.
func generateStepRunName(storyRunName, stepID string, now time.Time) string {
	suffix := fmt.Sprintf("%d", now.UnixMilli())
	base := fmt.Sprintf("%s-%s", storyRunName, stepID)
	name := fmt.Sprintf("%s-%s", base, suffix)
	if len(name) <= validation.DNS1123SubdomainMaxLength {
		return name
	}

	sum := sha1.Sum([]byte(base))
	hashStr := hex.EncodeToString(sum[:4]) // 8 hex characters for collision resistance

	allowance := validation.DNS1123SubdomainMaxLength - len(hashStr) - len(suffix) - 2 // separators
	if allowance < 1 {
		return fmt.Sprintf("%s-%s", hashStr, suffix)
	}

	truncated := base
	if len(truncated) > allowance {
		truncated = truncated[:allowance]
	}
	truncated = strings.Trim(truncated, "-")
	if len(truncated) == 0 {
		segment := hashStr
		if len(segment) > allowance {
			segment = segment[:allowance]
		}
		truncated = segment
	}
	if len(truncated) > allowance {
		truncated = truncated[:allowance]
	}

	return fmt.Sprintf("%s-%s-%s", truncated, hashStr, suffix)
}

// parseMaxMsgSizesFromEnv returns the recv and send message size limits.
func parseMaxMsgSizesFromEnv() (int, int) {
	const defaultMax = 10 * 1024 * 1024
	recvMax := defaultMax
	if v := os.Getenv(contracts.GRPCMaxRecvBytesEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			recvMax = n
		}
	}
	sendMax := defaultMax
	if v := os.Getenv(contracts.GRPCMaxSendBytesEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			sendMax = n
		}
	}
	return recvMax, sendMax
}

// parsePerMessageTimeoutFromEnv parses stream-wide per-message timeout or returns a default.
func parsePerMessageTimeoutFromEnv(logger logr.Logger) time.Duration {
	if v := os.Getenv(contracts.HubPerMessageTimeoutEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			logger.Info("Applying hub per-message timeout override", "envVar", contracts.HubPerMessageTimeoutEnv, "value", v)
			return d
		}
		logger.Info("Invalid hub per-message timeout value; ignoring override",
			"envVar", contracts.HubPerMessageTimeoutEnv, "value", v, "default", defaultPerMessageTimeout)
	}
	if v := os.Getenv(contracts.GRPCMessageTimeoutEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			logger.Info("Applying legacy gRPC message timeout override", "envVar", contracts.GRPCMessageTimeoutEnv, "value", v)
			return d
		}
		logger.Info("Invalid gRPC message timeout value; falling back to default",
			"envVar", contracts.GRPCMessageTimeoutEnv, "value", v, "default", defaultPerMessageTimeout)
	}
	logger.Info("No hub per-message timeout configured; applying default",
		"envVar", contracts.HubPerMessageTimeoutEnv, "default", defaultPerMessageTimeout)
	return defaultPerMessageTimeout
}

// tlsServerOptionFromEnv builds a TLS credentials option from environment variables.
func tlsServerOptionFromEnv(logger logr.Logger, allowInsecure bool) (grpc.ServerOption, error) {
	certFile := os.Getenv(contracts.HubTLSCertFileEnv)
	keyFile := os.Getenv(contracts.HubTLSKeyFileEnv)
	caFile := os.Getenv(contracts.HubCAFileEnv)

	// Default to known mount points if env vars are not explicitly set.
	if certFile == "" {
		certFile = fileIfExists(DefaultHubTLSCert)
	}
	if keyFile == "" {
		keyFile = fileIfExists(DefaultHubTLSKey)
	}
	if caFile == "" {
		caFile = fileIfExists(DefaultHubTLSCA)
	}

	useTLS := !allowInsecure
	if !useTLS {
		logger.Info(
			"Starting Hub without TLS (plaintext mode); set env var to tls and mount cert/key to enforce TLS",
			"securityModeEnv", contracts.TransportSecurityModeEnv,
		)
		return nil, nil
	}

	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("hub TLS required but %s/%s not available", contracts.HubTLSCertFileEnv, contracts.HubTLSKeyFileEnv)
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load hub TLS keypair: %w", err)
	}
	tlsConf := &tls.Config{Certificates: []tls.Certificate{cert}}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read hub CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("failed to append hub CA certs from %s", caFile)
		}
		tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
		tlsConf.ClientCAs = pool
	}
	return grpc.Creds(credentials.NewTLS(tlsConf)), nil
}

func fileIfExists(path string) string {
	if path == "" {
		return ""
	}
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		return path
	}
	return ""
}

// serverKeepaliveOptionFromEnv builds an optional keepalive server option.
func serverKeepaliveOptionFromEnv() (grpc.ServerOption, bool) {
	var kaParams keepalive.ServerParameters
	var have bool
	if v := os.Getenv(contracts.GRPCKeepaliveTimeEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			kaParams.Time = d
			have = true
		}
	}
	if v := os.Getenv(contracts.GRPCKeepaliveTimeoutEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			kaParams.Timeout = d
			have = true
		}
	}
	if !have {
		return nil, false
	}
	return grpc.KeepaliveParams(kaParams), true
}

// createBatchStepRun creates a StepRun for a batch (job-mode) engram step.
// If inputs is provided in the incoming packet, it is treated as pre-resolved and passed through.
//
// Workarounds for hybrid streaming→batch transitions:
//  1. Keep packet inputs small by using references/IDs instead of full payloads (recommended)
//  2. Use storage references in upstream engram outputs (store large data in S3, pass reference)
//  3. Split large payloads across multiple messages if feasible
//
// Design rationale: Hub-side storage offload requires careful error handling, retry logic, and cleanup
// on partial writes. The current design intentionally enforces the 1 MiB inline limit to maintain
// simplicity and reliability. Future enhancement may add Hub-side offload if hybrid patterns with
// large payloads become common. For now, the inline limit is enforced with a clear error message.
//
// Technical details: Kubernetes etcd has a ~1.5 MiB hard limit per object. We enforce 1 MiB for inputs
// to leave headroom for metadata, labels, and annotations in the StepRun CR.
func (s *Server) createBatchStepRun(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, inputs *structpb.Struct, upstreamStepID string) error {
	stepID := getStepID(step)
	name := generateStepRunName(storyRun.Name, stepID, time.Now())

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: storyRun.Namespace,
			Labels: map[string]string{
				"bubustack.io/storyrun":   storyRun.Name,
				"bubustack.io/story-name": story.Name,
				"bubustack.io/hybrid":     trueString,
			},
			Annotations: map[string]string{
				"bubustack.io/upstream-step": upstreamStepID,
			},
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: storyRun.Name}},
			StepID:      stepID,
			EngramRef:   step.Ref,
		},
	}

	// If we have per-packet evaluated inputs from the stream, pass them through and mark as resolved.
	if inputs != nil {
		b, err := json.Marshal(inputs.AsMap())
		if err != nil {
			return fmt.Errorf("failed to marshal inputs: %w", err)
		}

		stepRun.Spec.Input = &k8sruntime.RawExtension{Raw: b}
		if stepRun.Annotations == nil {
			stepRun.Annotations = map[string]string{}
		}
		stepRun.Annotations["bubustack.io/inputs-resolved"] = trueString
	} else if step.With != nil {
		// Fallback: use the step's static 'with' (will be resolved by controller CEL)
		stepRun.Spec.Input = step.With
	}

	return s.client.Create(ctx, stepRun)
}

func (s *Server) evaluatePrimitive(ctx context.Context, step *bubuv1alpha1.Step, payload *structpb.Struct, steps map[string]any, storyInputs map[string]any) (*structpb.Struct, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// For now, we only support CEL transform.
	// A real implementation would have a switch statement for different primitive types.
	if step.Type != enums.StepTypeTransform {
		s.log.Info("Skipping unsupported primitive", "type", step.Type)
		return payload, nil
	}
	if step.With == nil {
		return nil, errors.New("transform step is missing 'with' block")
	}

	result, err := s.evaluateWithBlock(ctx, step.With.Raw, buildCELVars(payload, storyInputs, steps))
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Server) evaluateEngramInputs(ctx context.Context, step *bubuv1alpha1.Step, payload *structpb.Struct, steps map[string]any, storyInputs map[string]any) (*structpb.Struct, error) {
	if step == nil || step.With == nil {
		return nil, nil
	}
	return s.evaluateWithBlock(ctx, step.With.Raw, buildCELVars(payload, storyInputs, steps))
}

func (s *Server) evaluateWithBlock(ctx context.Context, raw json.RawMessage, vars map[string]any) (*structpb.Struct, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}

	var withMap map[string]any
	if err := json.Unmarshal(raw, &withMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal 'with' block: %w", err)
	}

	rewritten := rewriteLegacyExpressions(withMap)
	if vars == nil {
		vars = map[string]any{}
	}
	if _, ok := vars["payload"]; !ok {
		vars["payload"] = map[string]any{}
	}
	if _, ok := vars["inputs"]; !ok {
		vars["inputs"] = vars["payload"]
	}
	if _, ok := vars["steps"]; !ok {
		vars["steps"] = map[string]any{}
	}

	if s.celEvaluator == nil {
		return nil, errors.New("cel evaluator not initialised")
	}

	resultMap, err := s.celEvaluator.ResolveWithInputs(ctx, rewritten, vars)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate 'with' block: %w", err)
	}

	return structpb.NewStruct(resultMap)
}

func rewriteLegacyExpressions(src map[string]any) map[string]any {
	rewritten := make(map[string]any, len(src))
	for k, v := range src {
		strVal, ok := v.(string)
		if !ok {
			rewritten[k] = v
			continue
		}
		if strings.HasPrefix(strVal, "{{") && strings.HasSuffix(strVal, "}}") {
			expr := strings.TrimSpace(strVal[2 : len(strVal)-2])
			if strings.Contains(expr, "payload.") {
				expr = strings.ReplaceAll(expr, "payload.", "inputs.")
				strVal = "{{ " + expr + " }}"
			}
		}
		rewritten[k] = strVal
	}
	return rewritten
}

func payloadAsMap(payload *structpb.Struct) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	return payload.AsMap()
}

func rawExtensionToMap(raw *k8sruntime.RawExtension) (map[string]any, error) {
	if raw == nil || len(raw.Raw) == 0 {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw.Raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func setStepOutputs(steps map[string]any, stepID string, payload, inputs *structpb.Struct) (map[string]any, bool) {
	if stepID == "" {
		return steps, false
	}
	if steps == nil {
		steps = make(map[string]any)
	}
	outputs := mergeMaps(payloadAsMap(payload), payloadAsMap(inputs))
	if len(outputs) == 0 {
		return steps, false
	}
	steps[stepID] = map[string]any{
		"outputs": outputs,
	}
	return steps, true
}

func buildCELVars(payload *structpb.Struct, storyInputs map[string]any, steps map[string]any) map[string]any {
	payloadMap := payloadAsMap(payload)
	vars := map[string]any{
		"payload": payloadMap,
		"inputs":  payloadMap,
	}
	if len(storyInputs) > 0 {
		vars["inputs"] = mergeMaps(payloadMap, storyInputs)
		vars["story"] = map[string]any{"inputs": storyInputs}
	}
	if len(steps) > 0 {
		vars["steps"] = steps
	}
	return vars
}

func mergeMaps(primary, secondary map[string]any) map[string]any {
	if len(primary) == 0 && len(secondary) == 0 {
		return map[string]any{}
	}
	merged := make(map[string]any, len(primary)+len(secondary))
	for k, v := range secondary {
		merged[k] = v
	}
	for k, v := range primary {
		merged[k] = v
	}
	return merged
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

func findNextSteps(story *bubuv1alpha1.Story, currentStepID string) (nextSteps []*bubuv1alpha1.Step, nextEngramStep *bubuv1alpha1.Step, err error) {
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

	for i := startIndex + 1; i < len(story.Spec.Steps); i++ {
		step := &story.Spec.Steps[i]
		if step.Ref != nil {
			nextEngramStep = step
			break
		}
		nextSteps = append(nextSteps, step)
	}

	return nextSteps, nextEngramStep, nil
}

func getStepID(step *bubuv1alpha1.Step) string {
	if step.ID != "" {
		return step.ID
	}
	return step.Name
}
