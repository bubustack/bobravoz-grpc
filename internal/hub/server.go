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
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bobravoz-grpc/internal/telemetry"
	grpc_metrics "github.com/bubustack/bobravoz-grpc/pkg/metrics"
	"github.com/bubustack/core/contracts"
	bootstrapruntime "github.com/bubustack/core/runtime/bootstrap"
	identity "github.com/bubustack/core/runtime/identity"
	stagemeta "github.com/bubustack/core/runtime/stage"
	"github.com/bubustack/core/templating"
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
	templateEvaluator *templating.Evaluator
	streamManager     *StreamManager
	storageManager    *storage.StorageManager
	perMessageTimeout time.Duration
	channelBufferSize int
	offloadedPolicy   string
	materializeEngram string
	closeOnce         sync.Once
}

// NewServer creates a new hub server.
func NewServer(ctx context.Context, k8sClient client.Client, templateCfg templating.Config, offloadedPolicy, materializeEngram string) (*Server, error) {
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
	templateEvaluator, err := templating.New(templateCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create template evaluator: %w", err)
	}
	return &Server{
		client:            k8sClient,
		cache:             cache,
		log:               logger,
		templateEvaluator: templateEvaluator,
		streamManager:     NewStreamManager(),
		storageManager:    storageMgr,
		channelBufferSize: getChannelBufferSize(),
		offloadedPolicy:   strings.TrimSpace(offloadedPolicy),
		materializeEngram: strings.TrimSpace(materializeEngram),
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
		if s.templateEvaluator != nil {
			s.templateEvaluator.Close()
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
		if k == metaMaterializeNextStep {
			continue
		}
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

func cloneStruct(src *structpb.Struct) *structpb.Struct {
	if src == nil {
		return nil
	}
	cloned, ok := proto.Clone(src).(*structpb.Struct)
	if !ok {
		return src
	}
	return cloned
}

// startHeartbeatSender reads BUBU_GRPC_HEARTBEAT_INTERVAL (default 10s), logs
// the effective cadence, and runs a ticker goroutine that calls
// streamManager.SendHeartbeats until the provided context is canceled
// (`internal/hub/server.go:299-335`).
func (s *Server) startHeartbeatSender(ctx context.Context) {
	interval := 10 * time.Second // Default interval, matches SDK
	overrideSource := "default"
	if v := os.Getenv(contracts.GRPCHeartbeatIntervalEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			interval = d
			overrideSource = fmt.Sprintf("env:%s", contracts.GRPCHeartbeatIntervalEnv)
		} else if err != nil {
			s.log.Error(err, "Invalid heartbeat interval override, using default", "value", v)
		} else {
			s.log.Info("Ignoring non-positive heartbeat interval override", "value", v)
		}
	}
	s.log.Info("Starting heartbeat sender", "interval", interval.String(), "overrideSource", overrideSource)
	grpc_metrics.RecordHubHeartbeatInterval(interval)

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
				if err := s.streamManager.SendHeartbeats(ctx); err != nil {
					grpc_metrics.RecordHubHeartbeatFailure()
					s.log.Error(err, "Failed to broadcast heartbeat batch")
				}
			}
		}
	}()
}

// Process is the bidirectional streaming RPC for the hub.
func (s *Server) Process(stream transportpb.HubService_ProcessServer) error {
	s.log.Info("New stream established")
	ctx := stream.Context()
	streamContract := bootstrapruntime.NewContractLogger(s.log, "hub").WithComponent("stream")
	streamContract.Start("register")
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
		streamContract.Failure("register", err)
		return err
	}
	s.log.Info("Incoming stream metadata snapshot", "metadata", md)

	storyRunName, storyRunNS, currentStepID, err := s.extractMetadata(md)
	if err != nil {
		s.log.Error(err, "Failed to extract metadata")
		streamContract.Failure("register", err)
		return err
	}
	meta := stagemeta.StoryRunMetadata(storyRunName, storyRunNS).WithStep(currentStepID)
	meta.Info(s.log, "Hub stream metadata extracted")

	meta.Info(s.log, "Registering stream")
	streamEntry := s.streamManager.AddStream(ctx, storyRunName, storyRunNS, currentStepID, stream)
	meta.Info(s.log, "Hub stream registered successfully")
	defer s.streamManager.RemoveStream(storyRunName, storyRunNS, currentStepID, streamEntry)
	streamContract = streamContract.WithStage(meta)
	streamContract.Success("register")

	// Handle incoming messages in this goroutine and return when the stream ends
	loopContract := bootstrapruntime.NewContractLogger(s.log, "hub").WithComponent("messageLoop").WithStage(meta)
	if err := s.messageLoop(ctx, stream, meta, loopContract); err != nil {
		return err
	}
	meta.Info(s.log, "Stream ended")
	return nil
}

// recvResult represents the result of a stream.Recv() operation
type recvResult struct {
	req *transportpb.ProcessRequest
	err error
}

// messageLoop serializes hub Process() traffic by feeding stream.Recv results
// through a single worker, ignores empty wrappers and heartbeats (while still
// recording metrics), and calls processPacket for each payload until the
// context is canceled or the client closes the stream
// (`internal/hub/server.go:382-452`).
func (s *Server) messageLoop(ctx context.Context, stream transportpb.HubService_ProcessServer, meta stagemeta.Metadata, contract bootstrapruntime.ContractLogger) (err error) {
	contract.Start("loop")
	defer func() {
		if err != nil {
			contract.Failure("loop", err)
		} else {
			contract.Success("loop")
		}
	}()
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
			meta.Info(s.log, "Stream context done", "err", ctx.Err())
			err = ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				meta.Info(s.log, "Stream deadline exceeded")
				err = status.Errorf(codes.DeadlineExceeded, "hub stream deadline exceeded: %v", err)
			}
			meta.Info(s.log, "Stream context done, closing message loop")
			return err

		case result, ok := <-recvCh:
			if !ok {
				// Channel closed unexpectedly
				meta.Info(s.log, "recvCh closed unexpectedly")
				err = io.EOF
				return err
			}

			if result.err != nil {
				if result.err == io.EOF {
					meta.Info(s.log, "Upstream closed the stream")
					err = nil
					return err
				}
				meta.Error(s.log, result.err, "Error receiving from stream")
				err = result.err
				return err
			}

			packet := result.req.GetPacket()
			if packet == nil {
				meta.Info(s.log, "Received empty packet wrapper")
				continue // ignore empty wrapper
			}
			metadataCount := len(packet.Metadata)
			meta.Info(s.log, "Received packet from hub", "metadataKeys", metadataCount)

			if !isHeartbeat(packet) {
				grpc_metrics.RecordHubMessageReceived(meta.StoryRun, meta.Step)
			} else {
				grpc_metrics.RecordHubHeartbeat(meta.StoryRun, meta.Step)
				meta.Info(s.log.V(1), "Heartbeat received")
			}

			// Process the packet (may be slow due to K8s API calls)
			if err := s.processPacket(ctx, meta.StoryRun, meta.Namespace, meta.Step, packet); err != nil {
				meta.Info(s.log, "processPacket failed",
					"metadataCount", metadataCount,
					"severity", "warn",
				)
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

	if nextStep := materializeNextStep(in.Metadata); nextStep != "" {
		if isHeartbeat(in) {
			s.log.V(1).Info("Ignoring heartbeat from materialize step", "storyRun", storyRunName, "currentStep", currentStepID)
			return nil
		}
		return s.handleMaterializeResult(ctx, storyRun, story, nextStep, in)
	}

	primitiveSteps, nextEngramStep, hotTransport, err := findNextSteps(story, currentStepID)
	if err != nil {
		if strings.Contains(err.Error(), "last step") {
			s.log.Info("End of pipeline reached for packet", "storyRun", storyRunName, "lastStep", currentStepID)
		} else {
			s.log.Error(err, "Could not determine next step", "storyRun", storyRunName, "currentStep", currentStepID)
		}
		return nil
	}

	// Debug logging for routing
	nextStepName := "<none>"
	if nextEngramStep != nil {
		nextStepName = getStepID(nextEngramStep)
	}
	s.log.Info("Routing packet via DAG", "storyRun", storyRunName, "from", currentStepID, "to", nextStepName, "primitiveSteps", len(primitiveSteps))

	storyInputs, inputsErr := rawExtensionToMap(storyRun.Spec.Inputs)
	if inputsErr != nil {
		s.log.Error(inputsErr, "Failed to parse story inputs; downstream templates may lack 'inputs'", "storyRun", storyRunName)
	}

	if s.handleHeartbeatPacket(ctx, storyRun, nextEngramStep, currentStepID, in) {
		return nil
	}

	s.log.Info("Received packet", "storyRun", storyRunName, "fromStep", currentStepID)

	if hotTransport && len(primitiveSteps) == 0 && !requiresDynamicInputs(nextEngramStep) {
		s.log.Info("Delivering packet via hot transport bypass", "storyRun", storyRunName, "from", currentStepID, "to", getStepID(nextEngramStep))
		return s.forwardHotPacket(ctx, storyRun, nextEngramStep, in)
	}

	processedPayload, stepVars, recorded, err := s.evaluatePrimitiveChain(ctx, storyRunName, currentStepID, primitiveSteps, in, storyInputs)
	if err != nil {
		return err
	}
	if !recorded {
		return nil
	}

	evaluatedInputs, deferred, err := s.evaluateNextEngramInputs(ctx, storyRunName, nextEngramStep, processedPayload, stepVars, storyInputs)
	if err != nil {
		var required *materializeRequired
		if errors.As(err, &required) {
			s.log.Info("Routing packet to materialize engram", "storyRun", storyRunName, "targetStep", getStepID(nextEngramStep))
			return s.routeToMaterialize(ctx, storyRun, story, nextEngramStep, processedPayload, in, required.Request())
		}
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

	// Media packets (Audio/Video/Binary) are valid even without Payload/Inputs
	// They carry data in their respective fields, not in the Payload field
	hasMediaData := packet.GetAudio() != nil || packet.GetVideo() != nil || packet.GetBinary() != nil

	if !recorded && !hasMediaData {
		s.log.V(1).Info("Packet produced no outputs and has no media data; skipping downstream evaluation",
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
		var required *materializeRequired
		if errors.As(err, &required) {
			return nil, false, err
		}
		var blocked *templating.ErrEvaluationBlocked
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

	// Debug logging for packet contents
	hasAudio := originalPacket.GetAudio() != nil
	hasVideo := originalPacket.GetVideo() != nil
	hasBinary := originalPacket.GetBinary() != nil
	audioLen := 0
	if hasAudio {
		audioLen = len(originalPacket.GetAudio().GetPcm())
	}
	s.log.Info("Hub forwarding packet",
		"storyRun", storyRun.Name,
		"to", nextEngramStepID,
		"hasAudio", hasAudio,
		"audioPcmLen", audioLen,
		"hasVideo", hasVideo,
		"hasBinary", hasBinary,
		"hasPayload", payload != nil,
		"hasInputs", evaluatedInputs != nil,
	)

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

func (s *Server) forwardHotPacket(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	nextEngramStep *bubuv1alpha1.Step,
	originalPacket *transportpb.DataPacket,
) error {
	if nextEngramStep == nil {
		return nil
	}
	nextEngramStepID := getStepID(nextEngramStep)

	// DEBUG: Log incoming packet audio state
	inHasAudio := originalPacket.GetAudio() != nil
	inAudioPcmLen := 0
	if inHasAudio {
		inAudioPcmLen = len(originalPacket.GetAudio().GetPcm())
	}
	s.log.Info("[HUB_FORWARD] Incoming packet",
		"storyRun", storyRun.Name,
		"from", originalPacket.Metadata["current-step-id"],
		"to", nextEngramStepID,
		"hasAudio", inHasAudio,
		"audioPcmLen", inAudioPcmLen)

	out := &transportpb.DataPacket{
		Metadata:   copyMetadataForStep(originalPacket.Metadata, storyRun.Name, storyRun.Namespace, nextEngramStepID),
		Payload:    cloneStruct(originalPacket.GetPayload()),
		Inputs:     cloneStruct(originalPacket.GetInputs()),
		Transports: cloneTransports(originalPacket.GetTransports()),
		Audio:      cloneAudioFrame(originalPacket.GetAudio()),
		Video:      cloneVideoFrame(originalPacket.GetVideo()),
		Binary:     cloneBinaryFrame(originalPacket.GetBinary()),
	}

	// DEBUG: Log outgoing packet audio state after cloning
	outHasAudio := out.GetAudio() != nil
	outAudioPcmLen := 0
	if outHasAudio {
		outAudioPcmLen = len(out.GetAudio().GetPcm())
	}
	s.log.Info("[HUB_FORWARD] Outgoing packet after clone",
		"storyRun", storyRun.Name,
		"to", nextEngramStepID,
		"hasAudio", outHasAudio,
		"audioPcmLen", outAudioPcmLen)

	if ok := s.streamManager.SendOrBuffer(ctx, storyRun.Name, storyRun.Namespace, nextEngramStepID, out); !ok {
		s.log.Info("Failed to deliver or buffer hot-path packet; dropping", "storyRun", storyRun.Name, "downstreamStep", nextEngramStepID)
		return status.Errorf(codes.ResourceExhausted, "downstream buffer full for step %q", nextEngramStepID)
	}
	return nil
}

func requiresDynamicInputs(step *bubuv1alpha1.Step) bool {
	if step == nil {
		return false
	}
	if step.Runtime != nil && len(step.Runtime.Raw) > 0 {
		return true
	}
	if step.With != nil && len(step.With.Raw) > 0 {
		return true
	}
	return false
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

	labels := identity.StoryRunSelectorLabels(storyRun.Name)
	labels[contracts.StoryNameLabelKey] = story.Name
	labels["bubustack.io/hybrid"] = trueString
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: storyRun.Namespace,
			Labels:    labels,
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
		// Fallback: use the step's static 'with' (will be resolved by controller templating)
		stepRun.Spec.Input = step.With
	}

	return s.client.Create(ctx, stepRun)
}

func (s *Server) evaluatePrimitive(ctx context.Context, step *bubuv1alpha1.Step, payload *structpb.Struct, steps map[string]any, storyInputs map[string]any) (*structpb.Struct, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Primitive evaluation is currently disabled. Keep payload unchanged.
	s.log.Info("Skipping primitive evaluation", "type", step.Type)
	return payload, nil
}

func (s *Server) evaluateEngramInputs(ctx context.Context, step *bubuv1alpha1.Step, payload *structpb.Struct, steps map[string]any, storyInputs map[string]any) (*structpb.Struct, error) {
	if step == nil {
		return nil, nil
	}

	// For realtime steps with runtime field, evaluate runtime configuration per-packet
	// This allows dynamic config referencing other step outputs
	if step.Runtime != nil && len(step.Runtime.Raw) > 0 {
		vars := buildRuntimeCELVars(payload, storyInputs, steps)
		return s.evaluateWithBlock(ctx, step.Runtime.Raw, vars)
	}

	// For batch steps or steps without runtime field, evaluate with block
	// This is the legacy/batch behavior
	if step.With != nil {
		vars := buildCELVars(payload, storyInputs, steps)
		return s.evaluateWithBlock(ctx, step.With.Raw, vars)
	}

	return nil, nil
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

	if s.templateEvaluator == nil {
		return nil, errors.New("template evaluator not initialised")
	}

	if s.shouldInjectOffloaded() {
		if stepsMap, ok := vars["steps"].(map[string]any); ok {
			if offloaded := detectOffloadedOutputRefs(string(raw), stepsMap); offloaded != nil {
				return nil, s.buildMaterializeError(rewritten, vars, offloaded.Reason)
			}
		}
	}

	resultMap, err := s.templateEvaluator.ResolveWithInputs(ctx, rewritten, vars)
	if err != nil {
		var offloaded *templating.ErrOffloadedDataUsage
		if errors.As(err, &offloaded) && s.shouldInjectOffloaded() {
			return nil, s.buildMaterializeError(rewritten, vars, offloaded.Reason)
		}
		return nil, fmt.Errorf("failed to evaluate template block: %w", err)
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

// buildRuntimeCELVars constructs the template evaluation context for runtime field evaluation.
// This is used for realtime/streaming steps where configuration needs to reference
// other step outputs on a per-packet basis.
//
// Available contexts:
//   - steps.*: Direct access to other step outputs (e.g., steps.transcribe.text)
//   - inputs.*: Story inputs (static, from StoryRun.Spec.Inputs)
//   - packet.*: Current packet payload data
func buildRuntimeCELVars(payload *structpb.Struct, storyInputs map[string]any, stepsRaw map[string]any) map[string]any {
	payloadMap := payloadAsMap(payload)
	vars := map[string]any{
		"packet": payloadMap,  // Current packet data
		"inputs": storyInputs, // Story inputs
	}

	if len(stepsRaw) > 0 {
		vars["steps"] = flattenStepOutputs(stepsRaw) // Flattened step outputs
	}

	return vars
}

// flattenStepOutputs extracts step outputs from the accumulated step variables
// and provides direct access without the .outputs nesting.
//
// Input format: steps["transcribe"] = {"outputs": {"text": "hello", "model": "gpt-4"}}
// Output format: steps["transcribe"] = {"text": "hello", "model": "gpt-4"}
//
// This allows cleaner template expressions:
//
//	{{ steps.transcribe.text }}
func flattenStepOutputs(stepsRaw map[string]any) map[string]any {
	flattened := make(map[string]any)

	for stepID, stepData := range stepsRaw {
		if stepMap, ok := stepData.(map[string]any); ok {
			if outputs, ok := stepMap["outputs"]; ok {
				// Flatten: steps.stepID = outputs (without .outputs wrapper)
				flattened[stepID] = outputs
			}
		}
	}

	return flattened
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

func findNextSteps(story *bubuv1alpha1.Story, currentStepID string) (nextSteps []*bubuv1alpha1.Step, nextEngramStep *bubuv1alpha1.Step, hotTransport bool, err error) {
	if story == nil {
		return nil, nil, false, fmt.Errorf("story is nil")
	}

	stepIndex := -1
	for i := range story.Spec.Steps {
		if getStepID(&story.Spec.Steps[i]) == currentStepID {
			stepIndex = i
			break
		}
	}
	if stepIndex == -1 {
		return nil, nil, false, fmt.Errorf("step %q not found in story", currentStepID)
	}
	if stepIndex+1 >= len(story.Spec.Steps) {
		return nil, nil, false, errors.New("current step is the last step")
	}

	currentStep := &story.Spec.Steps[stepIndex]
	currentTransport := currentStep.Transport
	hotTransport = isHotTransport(story, currentTransport)

	var downstream []*bubuv1alpha1.Step
	for i := stepIndex + 1; i < len(story.Spec.Steps); i++ {
		step := &story.Spec.Steps[i]
		if !dependsOnStep(step, currentStepID) {
			continue
		}
		if currentTransport != "" && step.Transport != currentTransport {
			continue
		}
		downstream = append(downstream, step)
		if step.Ref != nil {
			break
		}
	}

	if len(downstream) == 0 {
		return nil, nil, hotTransport, errors.New("current step is the last step")
	}

	for _, step := range downstream {
		if step.Ref != nil {
			nextEngramStep = step
			break
		}
		nextSteps = append(nextSteps, step)
	}

	return nextSteps, nextEngramStep, hotTransport, nil
}

func getStepID(step *bubuv1alpha1.Step) string {
	if step.ID != "" {
		return step.ID
	}
	return step.Name
}

func isHotTransport(story *bubuv1alpha1.Story, transportName string) bool {
	if story == nil || transportName == "" {
		return false
	}
	for _, status := range story.Status.Transports {
		if status.Name != transportName {
			continue
		}
		mode := status.Mode
		if mode == "" {
			mode = enums.TransportModeHot
		}
		return mode == enums.TransportModeHot
	}
	return false
}

func dependsOnStep(step *bubuv1alpha1.Step, currentStepID string) bool {
	if step == nil {
		return false
	}
	if len(step.Needs) == 0 {
		return true
	}
	for _, need := range step.Needs {
		if need == currentStepID {
			return true
		}
	}
	return false
}
