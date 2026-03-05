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
	"regexp"
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
	coretransport "github.com/bubustack/core/runtime/transport"
	"github.com/bubustack/core/templating"
	envelopecontract "github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
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
	metaJoinKey              = "bubu.join.key"
	metaEnvelopeMessageIDKey = "bubu.envelope.message_id"
	trueString               = "true"
	defaultPerMessageTimeout = 10 * time.Minute
	executeStoryPollInterval = 1 * time.Second

	DefaultHubTLSDir  = "/var/run/hub-tls"
	DefaultHubTLSCert = DefaultHubTLSDir + "/tls.crt"
	DefaultHubTLSKey  = DefaultHubTLSDir + "/tls.key"
	DefaultHubTLSCA   = DefaultHubTLSDir + "/ca.crt"

	lifecycleHookStepReadyEvent  = "steprun.ready"
	lifecycleHookStoryReadyEvent = "storyrun.ready"
)

var runtimeConditionRootRefPattern = regexp.MustCompile(`(?i)(^|[^a-z0-9_])\.?(packet|steps)($|[^a-z0-9_])`)

// Server is the gRPC hub server.
type Server struct {
	transportpb.UnimplementedHubServiceServer
	client                client.Client
	cache                 *storyCache
	log                   logr.Logger
	templateEvaluator     *templating.Evaluator
	streamManager         *StreamManager
	storageManager        *storage.StorageManager
	joinCache             *joinCache
	templateSchemaCache   *templateSchemaCache
	perMessageTimeout     time.Duration
	channelBufferSize     int
	offloadedPolicy       string
	materializeEngram     string
	maxDownstreamsHardCap int
	cycles                *cycleTracker
	closeOnce             sync.Once
	hookMu                sync.Mutex
	emittedHooks          map[string]struct{}
}

type skipSchemaValidationKey struct{}

func withSkipSchemaValidation(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipSchemaValidationKey{}, true)
}

func shouldSkipSchemaValidation(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	val := ctx.Value(skipSchemaValidationKey{})
	skip, ok := val.(bool)
	return ok && skip
}

type errStopPropagation struct {
	Reason string
}

func (e *errStopPropagation) Error() string {
	if e == nil {
		return "packet propagation stopped"
	}
	if strings.TrimSpace(e.Reason) == "" {
		return "packet propagation stopped"
	}
	return fmt.Sprintf("packet propagation stopped: %s", e.Reason)
}

// concurrencyMode returns the streaming concurrency mode for the given story.
// TODO(task-9): read from story.Spec.Transport.ConcurrencyMode once the CRD
// field is added. Until then, default to "parallel" (no behaviour change).
func (s *Server) concurrencyMode(_ *bubuv1alpha1.Story) string {
	return "parallel"
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
		client:                k8sClient,
		cache:                 cache,
		log:                   logger,
		templateEvaluator:     templateEvaluator,
		streamManager:         NewStreamManager(storageMgr),
		storageManager:        storageMgr,
		joinCache:             newJoinCache(logger),
		templateSchemaCache:   newTemplateSchemaCache(k8sClient),
		channelBufferSize:     getChannelBufferSize(),
		offloadedPolicy:       strings.TrimSpace(offloadedPolicy),
		materializeEngram:     strings.TrimSpace(materializeEngram),
		maxDownstreamsHardCap: getMaxDownstreamsHardCap(),
		cycles:                newCycleTracker(),
		emittedHooks:          make(map[string]struct{}),
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

func joinKeyFromPacket(packet *transportpb.DataPacket) string {
	if packet == nil || packet.Metadata == nil {
		return ""
	}
	if val := strings.TrimSpace(packet.Metadata[metaJoinKey]); val != "" {
		return val
	}
	if val := strings.TrimSpace(packet.Metadata[metaEnvelopeMessageIDKey]); val != "" {
		return val
	}
	return ""
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

func cloneStreamEnvelope(src *transportpb.StreamEnvelope) *transportpb.StreamEnvelope {
	if src == nil {
		return nil
	}
	return &transportpb.StreamEnvelope{
		StreamId:   src.GetStreamId(),
		Sequence:   src.GetSequence(),
		Partition:  src.GetPartition(),
		ChunkId:    src.GetChunkId(),
		ChunkIndex: src.GetChunkIndex(),
		ChunkCount: src.GetChunkCount(),
		ChunkBytes: src.GetChunkBytes(),
		TotalBytes: src.GetTotalBytes(),
	}
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
	if err := s.validateProtocolMetadata(md); err != nil {
		s.log.Error(err, "Incoming hub stream rejected due to protocol version")
		streamContract.Failure("register", err)
		return status.Error(codes.FailedPrecondition, err.Error())
	}

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
	if streamEntry == nil {
		err := status.Error(codes.ResourceExhausted, "hub max active streams reached")
		streamContract.Failure("register", err)
		return err
	}
	s.maybeEmitLifecycleHooks(ctx, storyRunName, storyRunNS, currentStepID)
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

func (s *Server) maybeEmitLifecycleHooks(ctx context.Context, storyRunName, storyRunNS, currentStepID string) {
	storyRun, story, err := s.getStoryAndRunWithRetry(ctx, storyRunName, storyRunNS)
	if err != nil {
		s.log.V(1).Info("Skipping lifecycle hook emission; failed to load story context",
			"storyRun", storyRunName,
			"namespace", storyRunNS,
			"step", currentStepID,
			"error", err,
		)
		return
	}

	stepReadyKey := lifecycleHookDedupKey(storyRun.Namespace, storyRun.Name, lifecycleHookStepReadyEvent, currentStepID)
	s.emitLifecycleHookOnce(ctx, stepReadyKey, storyRun, story, lifecycleHookStepReadyEvent, currentStepID)

	if !s.allStreamingStepStreamsConnected(story, storyRun.Name, storyRun.Namespace) {
		return
	}
	storyReadyKey := lifecycleHookDedupKey(storyRun.Namespace, storyRun.Name, lifecycleHookStoryReadyEvent, "")
	s.emitLifecycleHookOnce(ctx, storyReadyKey, storyRun, story, lifecycleHookStoryReadyEvent, currentStepID)
}

func (s *Server) emitLifecycleHookOnce(
	ctx context.Context,
	dedupKey string,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	eventName string,
	originStepID string,
) {
	if storyRun == nil || story == nil {
		return
	}
	if !s.claimLifecycleHook(dedupKey) {
		return
	}
	if sent := s.emitLifecycleHookEvent(ctx, storyRun, story, eventName, originStepID); sent {
		return
	}
	s.releaseLifecycleHook(dedupKey)
}

func (s *Server) emitLifecycleHookEvent(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	eventName string,
	originStepID string,
) bool {
	consumerIdx := lifecycleHookConsumerStepIndexes(story, eventName)
	if len(consumerIdx) == 0 {
		return false
	}
	storyInputs, err := rawExtensionToMap(storyRun.Spec.Inputs)
	if err != nil {
		s.log.Error(err, "Failed to parse story inputs while emitting lifecycle hook",
			"storyRun", storyRun.Name,
			"event", eventName,
		)
	}
	payload, err := lifecycleHookPayload(eventName, storyRun.Name, storyRun.Namespace, originStepID)
	if err != nil {
		s.log.Error(err, "Failed to build lifecycle hook payload",
			"storyRun", storyRun.Name,
			"event", eventName,
		)
		return false
	}

	sentAny := false
	for _, idx := range consumerIdx {
		step := &story.Spec.Steps[idx]
		stepID := getStepID(step)
		if stepID == "" || step.Ref == nil {
			continue
		}

		shouldRun, deferred, err := s.evaluateStepCondition(ctx, step.If, payload, nil, storyInputs, false)
		if err != nil {
			s.log.Error(err, "Failed to evaluate lifecycle hook condition",
				"storyRun", storyRun.Name,
				"step", stepID,
				"event", eventName,
			)
			continue
		}
		if deferred || !shouldRun {
			continue
		}

		evaluatedInputs, deferred, err := s.evaluateNextEngramInputs(ctx, storyRun.Name, story, step, payload, nil, storyInputs)
		if err != nil {
			var required *materializeRequired
			if errors.As(err, &required) {
				s.log.Info("Skipping lifecycle hook delivery; step requires materialization",
					"storyRun", storyRun.Name,
					"step", stepID,
					"event", eventName,
				)
				continue
			}
			s.log.Error(err, "Failed to evaluate lifecycle hook runtime inputs",
				"storyRun", storyRun.Name,
				"step", stepID,
				"event", eventName,
			)
			continue
		}
		if deferred {
			continue
		}

		packet := lifecycleHookPacket(payload, evaluatedInputs, storyRun.Name, storyRun.Namespace, stepID, eventName, originStepID)
		limits := resolveBufferLimitsForStep(ctx, s.client, story, step)
		flow := resolveFlowControlPolicyForStep(ctx, s.client, story, step, limits)
		delivery := resolveDeliveryPolicyForStep(ctx, s.client, story, step)
		partitioning := resolvePartitioningPolicyForStep(ctx, s.client, story, step)
		lifecycle := resolveLifecyclePolicyForStep(ctx, s.client, story, step)
		applyPartitioningPolicy(packet, partitioning)

		if ok := s.streamManager.SendOrBufferWithOptions(ctx, storyRun.Name, storyRun.Namespace, stepID, packet, StreamOptions{
			BufferLimits: limits,
			Flow:         flow,
			Delivery:     delivery,
			MaxInFlight:  lifecycle.maxInFlight,
			DrainTimeout: lifecycle.drainTimeout,
			DrainEnabled: lifecycle.drainEnabled,
		}); ok {
			sentAny = true
			s.log.Info("Lifecycle hook delivered",
				"storyRun", storyRun.Name,
				"step", stepID,
				"event", eventName,
			)
			continue
		}
		s.log.Info("Lifecycle hook delivery failed",
			"storyRun", storyRun.Name,
			"step", stepID,
			"event", eventName,
		)
	}
	return sentAny
}

func lifecycleHookPacket(
	payload *structpb.Struct,
	inputs *structpb.Struct,
	storyRunName, storyRunNS, stepID, eventName, originStepID string,
) *transportpb.DataPacket {
	meta := map[string]string{
		"source":      "hub",
		"provider":    "hub",
		"type":        eventName,
		"kind":        envelopecontract.KindHook,
		"hook.event":  eventName,
		"hook.source": "hub",
	}
	if originStepID != "" {
		meta["hook.step"] = originStepID
	}
	return &transportpb.DataPacket{
		Metadata: copyMetadataForStep(meta, storyRunName, storyRunNS, stepID),
		Payload:  cloneStruct(payload),
		Inputs:   cloneStruct(inputs),
	}
}

func lifecycleHookPayload(eventName, storyRunName, storyRunNS, originStepID string) (*structpb.Struct, error) {
	data := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"storyRun": map[string]any{
			"name":      storyRunName,
			"namespace": storyRunNS,
		},
	}
	if originStepID != "" {
		data["step"] = map[string]any{"name": originStepID}
	}
	hook := map[string]any{
		"version": envelopecontract.LatestVersion,
		"event":   eventName,
		"source":  "hub",
		"data":    data,
	}
	return structpb.NewStruct(map[string]any{
		"kind": envelopecontract.KindHook,
		"type": eventName,
		"hook": hook,
	})
}

func lifecycleHookConsumerStepIndexes(story *bubuv1alpha1.Story, eventName string) []int {
	if story == nil {
		return nil
	}
	needle := strings.ToLower(strings.TrimSpace(eventName))
	if needle == "" {
		return nil
	}
	out := make([]int, 0)
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if step == nil || step.Ref == nil || step.If == nil {
			continue
		}
		expr := strings.ToLower(strings.TrimSpace(*step.If))
		if expr == "" {
			continue
		}
		if strings.Contains(expr, needle) {
			out = append(out, i)
		}
	}
	return out
}

func (s *Server) allStreamingStepStreamsConnected(story *bubuv1alpha1.Story, storyRunName, storyRunNS string) bool {
	if story == nil {
		return false
	}
	hasStreamingStep := false
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if step == nil || step.Ref == nil {
			continue
		}
		if strings.TrimSpace(step.Transport) == "" {
			continue
		}
		stepID := getStepID(step)
		if stepID == "" {
			continue
		}
		hasStreamingStep = true
		if !s.streamManager.HasStream(storyRunName, storyRunNS, stepID) {
			return false
		}
	}
	return hasStreamingStep
}

func lifecycleHookDedupKey(storyRunNS, storyRunName, eventName, stepID string) string {
	base := fmt.Sprintf("%s/%s:%s", storyRunNS, storyRunName, eventName)
	if strings.TrimSpace(stepID) == "" {
		return base
	}
	return fmt.Sprintf("%s:%s", base, stepID)
}

func (s *Server) claimLifecycleHook(key string) bool {
	if strings.TrimSpace(key) == "" {
		return false
	}
	s.hookMu.Lock()
	defer s.hookMu.Unlock()
	if _, exists := s.emittedHooks[key]; exists {
		return false
	}
	s.emittedHooks[key] = struct{}{}
	return true
}

func (s *Server) releaseLifecycleHook(key string) {
	if strings.TrimSpace(key) == "" {
		return
	}
	s.hookMu.Lock()
	defer s.hookMu.Unlock()
	delete(s.emittedHooks, key)
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
	reassembler := newPacketChunkReassembler(defaultChunkReassemblyTTL, 0, 0)
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

			if flow := result.req.GetFlow(); flow != nil {
				s.streamManager.ApplyFlow(meta.StoryRun, meta.Namespace, meta.Step, flow)
			}

			packet := result.req.GetPacket()
			if packet == nil {
				meta.Info(s.log, "Received empty packet wrapper")
				continue // ignore empty wrapper
			}
			assembled, complete, err := reassembler.Add(packet)
			if err != nil {
				meta.Error(s.log, err, "Chunk reassembly failed")
				return err
			}
			if !complete {
				continue
			}
			packet = assembled
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

	// Pipeline cycle management: track active processing cycles per step so that
	// concurrency modes (cancelPrevious, serial) can cancel or gate new packets.
	cycleKey := storyRunNS + "/" + storyRunName + ":" + currentStepID
	mode := s.concurrencyMode(story)
	cycle := s.cycles.start(cycleKey, mode, ctx)
	defer func() {
		cycle.complete()
		s.cycles.remove(cycleKey, cycle.id)
	}()

	storyInputs, inputsErr := rawExtensionToMap(storyRun.Spec.Inputs)
	if inputsErr != nil {
		s.log.Error(inputsErr, "Failed to parse story inputs; downstream templates may lack 'inputs'", "storyRun", storyRunName)
	}

	sourceIndex, err := findStepIndex(story, currentStepID)
	if err != nil {
		s.log.Error(err, "Current step not found in story", "storyRun", storyRunName, "currentStep", currentStepID)
		return nil
	}
	currentStep := &story.Spec.Steps[sourceIndex]
	currentTransport := currentStep.Transport
	hotTransport := isHotTransport(story, currentTransport)

	routingPolicy := resolveRoutingPolicyForStep(ctx, s.client, story, currentStep)
	routingPolicy = s.clampMaxDownstreams(routingPolicy)
	routingPolicy = s.clampMaxDownstreams(routingPolicy)
	observabilityPolicy := resolveObservabilityPolicyForStep(ctx, s.client, story, currentStep)
	recordingPolicy := resolveRecordingPolicyForStep(ctx, s.client, story, currentStep)
	if !isHeartbeat(in) {
		var endSpan func()
		ctx, endSpan = maybeStartPacketSpan(ctx, observabilityPolicy, storyRunName, currentStepID, in)
		defer endSpan()
		if observabilityPolicy.metricsEnabled && observabilityPolicy.watermarkEnabled {
			if eventTime, ok := extractEventTime(in, observabilityPolicy.watermarkSource); ok {
				recordEventTimeMetrics(storyRunName, currentStepID, eventTime)
			}
		}
		if recordingPolicy.mode != "off" {
			maybeRecordStreamPacket(ctx, s.storageManager, storyRunName, storyRunNS, currentStepID, in, recordingPolicy)
		}
	}

	var fanOutGroup *errgroup.Group
	var fanOutCancel context.CancelFunc
	fanOutCtx := cycle.ctx
	if strings.EqualFold(routingPolicy.fanOut, "parallel") {
		fanOutCtx, fanOutCancel = context.WithCancel(cycle.ctx)
		fanOutGroup, fanOutCtx = errgroup.WithContext(fanOutCtx)
	}
	finalize := func(err error) error {
		if fanOutGroup == nil {
			return err
		}
		if err != nil && fanOutCancel != nil {
			fanOutCancel()
		}
		if waitErr := fanOutGroup.Wait(); waitErr != nil && err == nil {
			err = waitErr
		}
		return err
	}

	if s.handleHeartbeatPacket(ctx, storyRun, story, currentStepID, currentTransport, sourceIndex, in, storyInputs) {
		return finalize(nil)
	}

	s.log.Info("Received packet", "storyRun", storyRunName, "fromStep", currentStepID)

	stepVars, recorded := setStepOutputs(nil, currentStepID, in.Payload, in.Inputs)
	hasMediaData := in.GetAudio() != nil || in.GetVideo() != nil || in.GetBinary() != nil
	skipSchemaValidation := hasMediaData && !recorded
	if skipSchemaValidation {
		ctx = withSkipSchemaValidation(ctx)
	}

	// Validate outputs from the current step against EngramTemplate output schema.
	if !skipSchemaValidation && recorded && currentStep.Ref != nil {
		if sourceEngram, resolveErr := s.resolveEngramForStep(ctx, storyRun.Namespace, currentStep); resolveErr == nil {
			outputMap := mergeMaps(payloadAsMap(in.Payload), payloadAsMap(in.Inputs))
			if valErr := s.validateEngramOutputs(ctx, currentStepID, sourceEngram, outputMap); valErr != nil {
				return finalize(valErr)
			}
		}
	}

	// Media packets (Audio/Video/Binary) are valid even without Payload/Inputs
	// They carry data in their respective fields, not in the Payload field
	if !recorded && !hasMediaData {
		s.log.V(1).Info("Packet produced no outputs and has no media data; skipping downstream evaluation",
			"storyRun", storyRunName,
			"step", currentStepID,
		)
		return finalize(nil)
	}

	processedPayload := in.Payload
	cursorIndex := sourceIndex
	executedPrimitive := false
	fanOut := false
	var fanOutPayload *structpb.Struct
	var fanOutStepVars map[string]any
	routedAny := false
	joinID := joinKeyFromPacket(in)
	downstreamCount := 0

	for {
		nextStep, nextIndex := findNextDependentStep(story, currentStepID, currentTransport, cursorIndex)
		if nextStep == nil {
			if !routedAny {
				s.log.Info("End of pipeline reached for packet", "storyRun", storyRunName, "lastStep", currentStepID)
			}
			return finalize(nil)
		}
		cursorIndex = nextIndex

		if nextStep.Ref == nil {
			primitivePayload := processedPayload
			primitiveStepVars := stepVars
			if fanOut {
				if fanOutPayload != nil {
					primitivePayload = fanOutPayload
				}
				if fanOutStepVars != nil {
					primitiveStepVars = fanOutStepVars
				}
			}

			shouldRun, deferred, err := s.evaluateStepCondition(ctx, nextStep.If, primitivePayload, primitiveStepVars, storyInputs, false)
			if err != nil {
				stepID := getStepID(nextStep)
				s.log.Error(err, "Failed to evaluate primitive condition", "step", stepID)
				return finalize(err)
			}
			if deferred {
				return finalize(nil)
			}
			if !shouldRun {
				s.log.Info("Skipping primitive due to if=false", "storyRun", storyRunName, "step", getStepID(nextStep))
				continue
			}

			processedPayload, err = s.evaluatePrimitive(ctx, storyRun, nextStep, primitivePayload, primitiveStepVars, storyInputs)
			if err != nil {
				var stop *errStopPropagation
				if errors.As(err, &stop) {
					s.log.Info("Stopping packet propagation", "storyRun", storyRunName, "step", getStepID(nextStep), "reason", stop.Reason)
					return finalize(nil)
				}
				stepID := getStepID(nextStep)
				s.log.Error(err, "Failed to evaluate primitive", "step", stepID)
				return finalize(err)
			}
			executedPrimitive = true
			wasFanOut := fanOut
			updatedVars, recordedPrimitive := setStepOutputs(primitiveStepVars, getStepID(nextStep), processedPayload, nil)
			if wasFanOut {
				fanOutPayload = processedPayload
				fanOutStepVars = updatedVars
			} else {
				stepVars = updatedVars
				if nextStep.Type == enums.StepTypeParallel {
					fanOut = true
					fanOutPayload = processedPayload
					fanOutStepVars = cloneStepVars(stepVars)
				}
			}
			if !recordedPrimitive {
				s.log.V(1).Info("Primitive produced no outputs; continuing",
					"storyRun", storyRunName,
					"primitiveStep", getStepID(nextStep),
				)
			}
			continue
		}

		effectivePayload := processedPayload
		effectiveStepVars := stepVars
		if fanOut {
			if fanOutPayload != nil {
				effectivePayload = fanOutPayload
			}
			if fanOutStepVars != nil {
				effectiveStepVars = fanOutStepVars
			}
		}

		shouldRun, deferred, err := s.evaluateStepCondition(ctx, nextStep.If, effectivePayload, effectiveStepVars, storyInputs, false)
		if err != nil {
			stepID := getStepID(nextStep)
			s.log.Error(err, "Failed to evaluate step condition", "step", stepID)
			return finalize(err)
		}
		if deferred {
			return finalize(nil)
		}
		if !shouldRun {
			s.log.Info("Skipping step due to if=false", "storyRun", storyRunName, "step", getStepID(nextStep))
			continue
		}

		if len(nextStep.Needs) > 1 && joinID != "" {
			fanInPolicy := resolveFanInPolicyForStep(ctx, s.client, story, nextStep)
			joinedVars, ready := s.joinCache.record(
				joinKey{
					storyRunName: storyRun.Name,
					storyRunNS:   storyRun.Namespace,
					stepID:       getStepID(nextStep),
					joinID:       joinID,
				},
				currentStepID,
				outputsForJoin(stepVars, currentStepID, effectivePayload, in.Inputs),
				nextStep.Needs,
				fanInPolicy,
			)
			if !ready {
				s.log.V(1).Info("Join pending; waiting on other branches", "storyRun", storyRun.Name, "step", getStepID(nextStep), "join", joinID)
				continue
			}
			effectiveStepVars = joinedVars
		}

		allowRoute, deferredRoute, err := s.shouldRouteToStep(ctx, routingPolicy, effectivePayload, effectiveStepVars, storyInputs, getStepID(nextStep), false)
		if err != nil {
			return finalize(err)
		}
		if deferredRoute {
			return finalize(nil)
		}
		if !allowRoute {
			continue
		}

		if !fanOut && hotTransport && !executedPrimitive && !requiresDynamicInputs(nextStep) {
			if routingPolicy.maxDownstreams > 0 && downstreamCount >= routingPolicy.maxDownstreams {
				s.log.Info("Skipping downstream due to maxDownstreams", "storyRun", storyRunName, "step", getStepID(nextStep), "maxDownstreams", routingPolicy.maxDownstreams)
				continue
			}
			s.log.Info("Delivering packet via hot transport bypass", "storyRun", storyRunName, "from", currentStepID, "to", getStepID(nextStep))
			if err := s.forwardHotPacket(cycle.ctx, storyRun, story, nextStep, in); err != nil {
				return finalize(err)
			}
			downstreamCount++
			routedAny = true
			continue
		}

		evaluatedInputs, deferred, err := s.evaluateNextEngramInputs(ctx, storyRunName, story, nextStep, effectivePayload, effectiveStepVars, storyInputs)
		if err != nil {
			var required *materializeRequired
			if errors.As(err, &required) {
				s.log.Info("Routing packet to materialize engram", "storyRun", storyRunName, "targetStep", getStepID(nextStep))
				return finalize(s.routeToMaterialize(ctx, storyRun, story, nextStep, effectivePayload, in, required.Request()))
			}
			return finalize(err)
		}
		if deferred {
			return finalize(nil)
		}

		if routingPolicy.maxDownstreams > 0 && downstreamCount >= routingPolicy.maxDownstreams {
			s.log.Info("Skipping downstream due to maxDownstreams", "storyRun", storyRunName, "step", getStepID(nextStep), "maxDownstreams", routingPolicy.maxDownstreams)
			continue
		}
		if fanOutGroup != nil {
			step := nextStep
			inputs := evaluatedInputs
			payloadCopy := effectivePayload
			original := in
			fanOutGroup.Go(func() error {
				return s.routePacket(fanOutCtx, storyRun, story, step, inputs, currentStepID, payloadCopy, original)
			})
		} else {
			if err := s.routePacket(cycle.ctx, storyRun, story, nextStep, evaluatedInputs, currentStepID, effectivePayload, in); err != nil {
				return finalize(err)
			}
		}
		downstreamCount++
		routedAny = true
		continue
	}
}

func (s *Server) handleHeartbeatPacket(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	currentStepID string,
	currentTransport string,
	startIndex int,
	in *transportpb.DataPacket,
	storyInputs map[string]any,
) bool {
	if !isHeartbeat(in) {
		return false
	}
	s.log.V(1).Info("Forwarding heartbeat", "from", currentStepID, "storyRun", storyRun.Name)

	currentStep := findStepByID(story, currentStepID)
	routingPolicy := resolveRoutingPolicyForStep(ctx, s.client, story, currentStep)
	downstreamCount := 0
	cursorIndex := startIndex
	for {
		nextStep, nextIndex := findNextDependentStep(story, currentStepID, currentTransport, cursorIndex)
		if nextStep == nil {
			return true
		}
		cursorIndex = nextIndex

		if nextStep.Ref == nil {
			continue
		}

		shouldRun, _, err := s.evaluateStepCondition(ctx, nextStep.If, nil, nil, storyInputs, true)
		if err != nil {
			s.log.Error(err, "Failed to evaluate heartbeat condition", "step", getStepID(nextStep))
			return true
		}
		if !shouldRun {
			s.log.V(1).Info("Skipping heartbeat due to condition", "step", getStepID(nextStep), "storyRun", storyRun.Name)
			continue
		}

		allowRoute, deferredRoute, err := s.shouldRouteToStep(ctx, routingPolicy, nil, nil, storyInputs, getStepID(nextStep), true)
		if err != nil {
			s.log.Error(err, "Failed to evaluate routing rule for heartbeat", "step", getStepID(nextStep))
			return true
		}
		if deferredRoute {
			return true
		}
		if !allowRoute {
			continue
		}

		if routingPolicy.maxDownstreams > 0 && downstreamCount >= routingPolicy.maxDownstreams {
			s.log.Info("Skipping heartbeat due to maxDownstreams", "storyRun", storyRun.Name, "step", getStepID(nextStep), "maxDownstreams", routingPolicy.maxDownstreams)
			continue
		}
		nextEngramStepID := getStepID(nextStep)
		limits := resolveBufferLimitsForStep(ctx, s.client, story, nextStep)
		flow := resolveFlowControlPolicyForStep(ctx, s.client, story, nextStep, limits)
		delivery := resolveDeliveryPolicyForStep(ctx, s.client, story, nextStep)
		partitioning := resolvePartitioningPolicyForStep(ctx, s.client, story, nextStep)
		lifecycle := resolveLifecyclePolicyForStep(ctx, s.client, story, nextStep)
		heartbeatPacket := &transportpb.DataPacket{
			Metadata: copyMetadataForStep(in.Metadata, storyRun.Name, storyRun.Namespace, nextEngramStepID),
			Payload:  in.Payload,
			Audio:    cloneAudioFrame(in.GetAudio()),
			Video:    cloneVideoFrame(in.GetVideo()),
			Binary:   cloneBinaryFrame(in.GetBinary()),
		}
		applyPartitioningPolicy(heartbeatPacket, partitioning)
		if ok := s.streamManager.SendOrBufferWithOptions(ctx, storyRun.Name, storyRun.Namespace, nextEngramStepID, heartbeatPacket, StreamOptions{
			BufferLimits: limits,
			Flow:         flow,
			Delivery:     delivery,
			MaxInFlight:  lifecycle.maxInFlight,
			DrainTimeout: lifecycle.drainTimeout,
			DrainEnabled: lifecycle.drainEnabled,
		}); !ok {
			s.log.Info("Failed to deliver or buffer heartbeat; dropping", "downstreamStep", nextEngramStepID, "reason", "buffer_full")
		}
		downstreamCount++
		continue
	}
}

func (s *Server) evaluatePrimitiveChain(
	ctx context.Context,
	storyRunName string,
	primitiveSteps []*bubuv1alpha1.Step,
	payload *structpb.Struct,
	stepVars map[string]any,
	storyInputs map[string]any,
) (*structpb.Struct, map[string]any, bool, error) {
	processedPayload := payload
	for _, primitiveStep := range primitiveSteps {
		if primitiveStep.Ref != nil {
			continue
		}

		shouldRun, deferred, err := s.evaluateStepCondition(ctx, primitiveStep.If, processedPayload, stepVars, storyInputs, false)
		if err != nil {
			stepID := getStepID(primitiveStep)
			s.log.Error(err, "Failed to evaluate primitive condition", "step", stepID)
			return nil, nil, false, status.Errorf(codes.Internal, "failed to evaluate primitive condition for step %q: %v", stepID, err)
		}
		if deferred {
			s.log.V(1).Info("Deferring packet; primitive condition blocked", "step", getStepID(primitiveStep))
			return processedPayload, stepVars, false, nil
		}
		if !shouldRun {
			s.log.V(1).Info("Skipping primitive due to if=false", "step", getStepID(primitiveStep))
			continue
		}

		processedPayload, err = s.evaluatePrimitive(ctx, nil, primitiveStep, processedPayload, stepVars, storyInputs)
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

func (s *Server) evaluateStepCondition(
	ctx context.Context,
	expr *string,
	payload *structpb.Struct,
	stepVars map[string]any,
	storyInputs map[string]any,
	isHeartbeat bool,
) (bool, bool, error) {
	if expr == nil {
		return true, false, nil
	}
	trimmed := strings.TrimSpace(*expr)
	if trimmed == "" {
		return true, false, nil
	}
	if s.templateEvaluator == nil {
		return false, false, errors.New("template evaluator not initialised")
	}

	if isStaticCondition(trimmed) {
		vars := buildStaticConditionVars(storyInputs)
		ok, err := s.templateEvaluator.EvaluateCondition(ctx, trimmed, vars)
		if err != nil {
			return false, false, err
		}
		return ok, false, nil
	}

	if err := templating.ValidateTemplateString(trimmed, streamingRuntimeScope()); err != nil {
		return false, false, err
	}

	if isHeartbeat {
		// Heartbeat packets should not be blocked by runtime predicates.
		return true, false, nil
	}

	vars := buildRuntimeCELVars(payload, storyInputs, stepVars)
	ok, err := s.templateEvaluator.EvaluateCondition(ctx, trimmed, vars)
	if err != nil {
		var blocked *templating.ErrEvaluationBlocked
		if errors.As(err, &blocked) {
			return false, true, nil
		}
		return false, false, err
	}
	return ok, false, nil
}

func (s *Server) shouldRouteToStep(
	ctx context.Context,
	policy routingPolicy,
	payload *structpb.Struct,
	stepVars map[string]any,
	storyInputs map[string]any,
	downstreamStepID string,
	isHeartbeat bool,
) (bool, bool, error) {
	if len(policy.rules) == 0 {
		return true, false, nil
	}
	matchedAllow := false
	matchedDeny := false
	for _, rule := range policy.rules {
		if !rule.target.matches(downstreamStepID) {
			continue
		}
		ok, deferred, err := s.evaluateStepCondition(ctx, rule.when, payload, stepVars, storyInputs, isHeartbeat)
		if err != nil {
			return false, false, err
		}
		if deferred {
			return false, true, nil
		}
		if !ok {
			continue
		}
		switch rule.action {
		case routingRuleDeny:
			matchedDeny = true
		case routingRuleAllow:
			matchedAllow = true
		}
	}
	if matchedDeny {
		return false, false, nil
	}
	if policy.hasAllowRules {
		return matchedAllow, false, nil
	}
	return true, false, nil
}

func isStaticCondition(expr string) bool {
	scope := templating.NewExpressionScope("if-static", false, false, true, templating.RootInputs)
	if templating.ValidateTemplateString(expr, scope) != nil {
		return false
	}
	// Guard against scope-misclassification for runtime roots used inside
	// helpers like index/default (e.g. index .steps "context-user").
	return !runtimeConditionRootRefPattern.MatchString(expr)
}

func streamingStaticScope() templating.ExpressionScope {
	return templating.NewExpressionScope("streaming-static", false, false, true, templating.RootInputs)
}

func streamingRuntimeScope() templating.ExpressionScope {
	return templating.NewExpressionScope("streaming-runtime", true, true, true, templating.RootInputs, templating.RootSteps, templating.RootPacket)
}

func buildStaticConditionVars(storyInputs map[string]any) map[string]any {
	vars := map[string]any{
		"inputs": storyInputs,
	}
	if len(storyInputs) > 0 {
		vars["story"] = map[string]any{"inputs": storyInputs}
	}
	return vars
}

func resolveStreamingRetryPolicy(step *bubuv1alpha1.Step, story *bubuv1alpha1.Story) *bubuv1alpha1.RetryPolicy {
	if step != nil && step.Execution != nil && step.Execution.Retry != nil {
		return step.Execution.Retry.DeepCopy()
	}
	if story == nil || story.Spec.Policy == nil || story.Spec.Policy.Retries == nil || story.Spec.Policy.Retries.StepRetryPolicy == nil {
		return nil
	}
	return story.Spec.Policy.Retries.StepRetryPolicy.DeepCopy()
}

func (s *Server) contextWithStepTimeout(ctx context.Context, step *bubuv1alpha1.Step, story *bubuv1alpha1.Story) (context.Context, context.CancelFunc) {
	timeout := s.resolveStreamingStepTimeout(step, story)
	if timeout <= 0 {
		return ctx, nil
	}
	if ctx == nil {
		return context.WithTimeout(context.Background(), timeout)
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= timeout {
			return ctx, nil
		}
	}
	return context.WithTimeout(ctx, timeout)
}

func (s *Server) resolveStreamingStepTimeout(step *bubuv1alpha1.Step, story *bubuv1alpha1.Story) time.Duration {
	if step != nil && step.Execution != nil && step.Execution.Timeout != nil {
		if parsed, err := parsePositiveDuration(*step.Execution.Timeout); err == nil {
			return parsed
		} else {
			s.log.Error(err, "Invalid step timeout override; ignoring", "step", getStepID(step))
		}
	}
	if story != nil && story.Spec.Policy != nil && story.Spec.Policy.Timeouts != nil && story.Spec.Policy.Timeouts.Step != nil {
		if parsed, err := parsePositiveDuration(*story.Spec.Policy.Timeouts.Step); err == nil {
			return parsed
		} else {
			s.log.Error(err, "Invalid story step timeout; ignoring", "story", story.Name)
		}
	}
	return 0
}

func parsePositiveDuration(raw string) (time.Duration, error) {
	parsed, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("parse duration: %w", err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	return parsed, nil
}

func (s *Server) evaluateNextEngramInputs(
	ctx context.Context,
	storyRunName string,
	story *bubuv1alpha1.Story,
	nextEngramStep *bubuv1alpha1.Step,
	payload *structpb.Struct,
	stepVars map[string]any,
	storyInputs map[string]any,
) (*structpb.Struct, bool, error) {
	if nextEngramStep == nil {
		return nil, false, nil
	}

	evaluatedInputs, err := s.evaluateEngramInputs(ctx, story, nextEngramStep, payload, stepVars, storyInputs)
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

	defaultedInputs, err := s.validateEngramInputs(ctx, storyRun, nextEngramStep, nextEngram, evaluatedInputs)
	if err != nil {
		return err
	}
	evaluatedInputs = defaultedInputs

	handled, err := s.handleBatchEngramIfNeeded(ctx, storyRun, story, nextEngramStep, nextEngram, evaluatedInputs, currentStepID)
	if handled || err != nil {
		return err
	}

	return s.forwardToRealtimeStep(ctx, storyRun, story, nextEngramStep, payload, evaluatedInputs, originalPacket)
}

func (s *Server) validateEngramInputs(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	nextEngramStep *bubuv1alpha1.Step,
	engram *bubuv1alpha1.Engram,
	evaluatedInputs *structpb.Struct,
) (*structpb.Struct, error) {
	if shouldSkipSchemaValidation(ctx) {
		return evaluatedInputs, nil
	}
	if engram == nil || nextEngramStep == nil {
		return evaluatedInputs, nil
	}
	if engram.Spec.Mode == enums.WorkloadModeJob {
		return evaluatedInputs, nil
	}

	templateName := strings.TrimSpace(engram.Spec.TemplateRef.Name)
	if templateName == "" {
		return nil, status.Errorf(codes.Internal, "engram %q is missing templateRef.name", engram.Name)
	}

	if s.templateSchemaCache == nil {
		return nil, status.Errorf(codes.Internal, "template schema cache not initialized")
	}
	schema, schemaName, schemaRaw, err := s.templateSchemaCache.Get(ctx, templateName)
	if err != nil {
		shouldSkip := apierrors.IsNotFound(err) ||
			apierrors.IsTimeout(err) ||
			errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, context.Canceled) ||
			k8sruntime.IsNotRegisteredError(err)
		if shouldSkip {
			s.log.Error(err, "Failed to resolve engram template; skipping input validation",
				"step", getStepID(nextEngramStep),
				"template", templateName,
			)
			return evaluatedInputs, nil
		}
		return nil, status.Errorf(codes.Unavailable, "failed to resolve engram template %q: %v", templateName, err)
	}
	if schema == nil {
		return evaluatedInputs, nil
	}

	inputMap := map[string]any{}
	if evaluatedInputs != nil {
		inputMap = evaluatedInputs.AsMap()
	}

	// Apply JSON Schema defaults before validation (matches batch path behavior).
	if len(schemaRaw) > 0 {
		defaulted, defaultErr := applySchemaDefaults(schemaRaw, inputMap)
		if defaultErr != nil {
			s.log.Error(defaultErr, "Failed to apply schema defaults; continuing with original inputs",
				"step", getStepID(nextEngramStep),
				"template", templateName,
			)
		} else {
			inputMap = defaulted
			// Rebuild the protobuf struct from the defaulted map.
			rebuilt, rebuildErr := structpb.NewStruct(inputMap)
			if rebuildErr != nil {
				s.log.Error(rebuildErr, "Failed to rebuild struct from defaulted inputs; continuing with original",
					"step", getStepID(nextEngramStep),
				)
			} else {
				evaluatedInputs = rebuilt
			}
		}
	}

	inputBytes, err := json.Marshal(inputMap)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal inputs for step %q: %v", getStepID(nextEngramStep), err)
	}
	if len(inputBytes) == 0 {
		inputBytes = []byte("{}")
	}
	if err := validateJSONAgainstSchema(inputBytes, schema, schemaName); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "inputs for step %q failed schema validation: %v", getStepID(nextEngramStep), err)
	}
	return evaluatedInputs, nil
}

// validateEngramOutputs validates packet outputs against the EngramTemplate output schema.
// Returns nil if no output schema is defined or validation passes.
func (s *Server) validateEngramOutputs(
	ctx context.Context,
	stepID string,
	engram *bubuv1alpha1.Engram,
	outputs map[string]any,
) error {
	if engram == nil || len(outputs) == 0 {
		return nil
	}

	templateName := strings.TrimSpace(engram.Spec.TemplateRef.Name)
	if templateName == "" || s.templateSchemaCache == nil {
		return nil
	}

	schema, schemaName, err := s.templateSchemaCache.GetOutputSchema(ctx, templateName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		s.log.Error(err, "Failed to resolve output schema; skipping validation", "step", stepID, "template", templateName)
		return nil
	}
	if schema == nil {
		return nil
	}

	outputBytes, err := json.Marshal(outputs)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal outputs for step %q: %v", stepID, err)
	}
	if len(outputBytes) == 0 {
		outputBytes = []byte("{}")
	}
	if err := validateJSONAgainstSchema(outputBytes, schema, schemaName); err != nil {
		s.log.Error(err, "Step outputs failed schema validation",
			"step", stepID,
			"template", templateName,
		)
		return status.Errorf(codes.InvalidArgument, "outputs for step %q failed schema validation: %v", stepID, err)
	}
	return nil
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
	story *bubuv1alpha1.Story,
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
		Envelope:   cloneStreamEnvelope(originalPacket.GetEnvelope()),
		Audio:      cloneAudioFrame(originalPacket.GetAudio()),
		Video:      cloneVideoFrame(originalPacket.GetVideo()),
		Binary:     cloneBinaryFrame(originalPacket.GetBinary()),
	}

	retryPolicy := resolveStreamingRetryPolicy(nextEngramStep, story)
	sendCtx, cancel := s.contextWithStepTimeout(ctx, nextEngramStep, story)
	if cancel != nil {
		defer cancel()
	}
	limits := resolveBufferLimitsForStep(ctx, s.client, story, nextEngramStep)
	flow := resolveFlowControlPolicyForStep(ctx, s.client, story, nextEngramStep, limits)
	delivery := resolveDeliveryPolicyForStep(ctx, s.client, story, nextEngramStep)
	partitioning := resolvePartitioningPolicyForStep(ctx, s.client, story, nextEngramStep)
	lifecycle := resolveLifecyclePolicyForStep(ctx, s.client, story, nextEngramStep)
	applyPartitioningPolicy(out, partitioning)
	if ok := s.streamManager.SendOrBufferWithOptions(sendCtx, storyRun.Name, storyRun.Namespace, nextEngramStepID, out, StreamOptions{
		RetryPolicy:  retryPolicy,
		BufferLimits: limits,
		Flow:         flow,
		Delivery:     delivery,
		MaxInFlight:  lifecycle.maxInFlight,
		DrainTimeout: lifecycle.drainTimeout,
		DrainEnabled: lifecycle.drainEnabled,
	}); !ok {
		s.log.Info("Failed to deliver or buffer packet; dropping and closing stream", "storyRun", storyRun.Name, "downstreamStep", nextEngramStepID, "reason", "buffer_exhausted")
		return status.Errorf(codes.ResourceExhausted, "downstream buffer full for step %q", nextEngramStepID)
	}
	return nil
}

func (s *Server) forwardHotPacket(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
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
		Envelope:   cloneStreamEnvelope(originalPacket.GetEnvelope()),
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

	retryPolicy := resolveStreamingRetryPolicy(nextEngramStep, story)
	sendCtx, cancel := s.contextWithStepTimeout(ctx, nextEngramStep, story)
	if cancel != nil {
		defer cancel()
	}
	limits := resolveBufferLimitsForStep(ctx, s.client, story, nextEngramStep)
	flow := resolveFlowControlPolicyForStep(ctx, s.client, story, nextEngramStep, limits)
	delivery := resolveDeliveryPolicyForStep(ctx, s.client, story, nextEngramStep)
	partitioning := resolvePartitioningPolicyForStep(ctx, s.client, story, nextEngramStep)
	lifecycle := resolveLifecyclePolicyForStep(ctx, s.client, story, nextEngramStep)
	applyPartitioningPolicy(out, partitioning)
	if ok := s.streamManager.SendOrBufferWithOptions(sendCtx, storyRun.Name, storyRun.Namespace, nextEngramStepID, out, StreamOptions{
		RetryPolicy:  retryPolicy,
		BufferLimits: limits,
		Flow:         flow,
		Delivery:     delivery,
		MaxInFlight:  lifecycle.maxInFlight,
		DrainTimeout: lifecycle.drainTimeout,
		DrainEnabled: lifecycle.drainEnabled,
	}); !ok {
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

func (s *Server) evaluatePrimitive(ctx context.Context, storyRun *runsv1alpha1.StoryRun, step *bubuv1alpha1.Step, payload *structpb.Struct, steps map[string]any, storyInputs map[string]any) (*structpb.Struct, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if step == nil {
		return payload, nil
	}

	switch step.Type {
	case enums.StepTypeCondition:
		return payload, nil
	case enums.StepTypeParallel:
		return payload, nil
	case enums.StepTypeSleep:
		return s.evaluateSleepPrimitive(ctx, step, payload, steps, storyInputs)
	case enums.StepTypeStop:
		return nil, s.evaluateStopPrimitive(ctx, step, payload, steps, storyInputs)
	case enums.StepTypeExecuteStory:
		return s.evaluateExecuteStoryPrimitive(ctx, storyRun, step, payload, steps, storyInputs)
	default:
		s.log.Info("Skipping primitive evaluation", "type", step.Type)
		return payload, nil
	}
}

func (s *Server) evaluateSleepPrimitive(
	ctx context.Context,
	step *bubuv1alpha1.Step,
	payload *structpb.Struct,
	steps map[string]any,
	storyInputs map[string]any,
) (*structpb.Struct, error) {
	cfg, err := s.resolvePrimitiveWith(ctx, step, payload, steps, storyInputs)
	if err != nil {
		return nil, err
	}
	var durationStr string
	if val, ok := cfg["duration"].(string); ok {
		durationStr = val
	} else if val, ok := cfg["for"].(string); ok {
		durationStr = val
	}
	if strings.TrimSpace(durationStr) == "" {
		return payload, nil
	}
	d, err := time.ParseDuration(durationStr)
	if err != nil {
		return nil, fmt.Errorf("sleep duration invalid: %w", err)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return payload, nil
	}
}

func (s *Server) evaluateStopPrimitive(
	ctx context.Context,
	step *bubuv1alpha1.Step,
	payload *structpb.Struct,
	steps map[string]any,
	storyInputs map[string]any,
) error {
	cfg, err := s.resolvePrimitiveWith(ctx, step, payload, steps, storyInputs)
	if err != nil {
		return err
	}
	message := "stop primitive requested"
	if val, ok := cfg["message"].(string); ok && strings.TrimSpace(val) != "" {
		message = val
	}
	return &errStopPropagation{Reason: message}
}

func (s *Server) evaluateExecuteStoryPrimitive(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	step *bubuv1alpha1.Step,
	payload *structpb.Struct,
	steps map[string]any,
	storyInputs map[string]any,
) (*structpb.Struct, error) {
	if storyRun == nil {
		return payload, fmt.Errorf("executeStory requires a parent StoryRun")
	}
	if step.With == nil {
		return payload, fmt.Errorf("executeStory step %q requires a with block", getStepID(step))
	}

	type executeStoryWith struct {
		StoryRef          *refs.ObjectReference    `json:"storyRef"`
		WaitForCompletion *bool                    `json:"waitForCompletion,omitempty"`
		With              *k8sruntime.RawExtension `json:"with,omitempty"`
	}
	var cfg executeStoryWith
	if err := json.Unmarshal(step.With.Raw, &cfg); err != nil {
		return payload, fmt.Errorf("executeStory step %q invalid with block: %w", getStepID(step), err)
	}
	if cfg.StoryRef == nil || cfg.StoryRef.Name == "" {
		return payload, fmt.Errorf("executeStory step %q missing storyRef", getStepID(step))
	}

	inputsRaw, err := s.resolveExecuteStoryInputs(ctx, cfg.With, payload, steps, storyInputs)
	if err != nil {
		return payload, err
	}

	subRunName := generateStepRunName(storyRun.Name, getStepID(step), time.Now())
	targetNS := storyRun.Namespace
	if cfg.StoryRef.Namespace != nil && strings.TrimSpace(*cfg.StoryRef.Namespace) != "" {
		targetNS = *cfg.StoryRef.Namespace
	}
	ref := refs.StoryReference{
		ObjectReference: refs.ObjectReference{
			Name:      cfg.StoryRef.Name,
			Namespace: &targetNS,
		},
	}
	subRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      subRunName,
			Namespace: storyRun.Namespace,
			Labels: map[string]string{
				contracts.ParentStoryRunLabel: storyRun.Name,
				contracts.ParentStepLabel:     getStepID(step),
			},
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: ref,
			Inputs:   inputsRaw,
		},
	}
	if storyRun.UID != "" {
		controller := true
		subRun.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: runsv1alpha1.GroupVersion.String(),
				Kind:       "StoryRun",
				Name:       storyRun.Name,
				UID:        storyRun.UID,
				Controller: &controller,
			},
		}
	}
	if err := s.client.Create(ctx, subRun); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return payload, fmt.Errorf("executeStory create failed: %w", err)
		}
	}

	waitForCompletion := cfg.WaitForCompletion != nil && *cfg.WaitForCompletion
	if waitForCompletion {
		s.log.Info("executeStory waiting for sub-story completion", "storyRun", storyRun.Name, "step", getStepID(step), "subRun", subRunName)
		subRun, err := s.waitForSubStoryCompletion(ctx, types.NamespacedName{Namespace: storyRun.Namespace, Name: subRunName})
		if err != nil {
			return payload, err
		}
		if subRun.Status.Phase == enums.PhaseSucceeded || subRun.Status.Phase == enums.PhaseSkipped {
			return payload, nil
		}
		message := subRun.Status.Message
		if strings.TrimSpace(message) == "" {
			message = fmt.Sprintf("Sub-story run '%s' completed with phase %s", subRun.Name, subRun.Status.Phase)
		}
		return payload, fmt.Errorf("executeStory step %q failed: %s", getStepID(step), message)
	}
	return payload, nil
}

func (s *Server) waitForSubStoryCompletion(ctx context.Context, key types.NamespacedName) (*runsv1alpha1.StoryRun, error) {
	ticker := time.NewTicker(executeStoryPollInterval)
	defer ticker.Stop()
	for {
		var subRun runsv1alpha1.StoryRun
		if err := s.client.Get(ctx, key, &subRun); err != nil {
			if apierrors.IsNotFound(err) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-ticker.C:
					continue
				}
			}
			return nil, fmt.Errorf("executeStory waitForCompletion get failed: %w", err)
		}
		if subRun.Status.Phase.IsTerminal() && subRun.Status.Phase != "" {
			return &subRun, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) resolvePrimitiveWith(
	ctx context.Context,
	step *bubuv1alpha1.Step,
	payload *structpb.Struct,
	steps map[string]any,
	storyInputs map[string]any,
) (map[string]any, error) {
	if step == nil || step.With == nil || len(step.With.Raw) == 0 {
		return map[string]any{}, nil
	}
	vars := buildRuntimeCELVars(payload, storyInputs, steps)
	resolved, err := s.evaluateWithBlock(ctx, step.With.Raw, vars)
	if err != nil {
		return nil, err
	}
	if resolved == nil {
		return map[string]any{}, nil
	}
	return resolved.AsMap(), nil
}

func (s *Server) resolveExecuteStoryInputs(
	ctx context.Context,
	raw *k8sruntime.RawExtension,
	payload *structpb.Struct,
	steps map[string]any,
	storyInputs map[string]any,
) (*k8sruntime.RawExtension, error) {
	if raw == nil || len(raw.Raw) == 0 {
		return nil, nil
	}
	vars := buildRuntimeCELVars(payload, storyInputs, steps)
	resolved, err := s.evaluateWithBlock(ctx, raw.Raw, vars)
	if err != nil {
		return nil, fmt.Errorf("executeStory inputs evaluation failed: %w", err)
	}
	if resolved == nil {
		return nil, nil
	}
	b, err := json.Marshal(resolved.AsMap())
	if err != nil {
		return nil, fmt.Errorf("executeStory inputs marshal failed: %w", err)
	}
	return &k8sruntime.RawExtension{Raw: b}, nil
}

func cloneStepVars(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (s *Server) evaluateEngramInputs(ctx context.Context, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, payload *structpb.Struct, steps map[string]any, storyInputs map[string]any) (*structpb.Struct, error) {
	if step == nil {
		return nil, nil
	}
	isStreaming := story != nil && story.Spec.Pattern == enums.StreamingPattern

	// For realtime steps with runtime field, evaluate runtime configuration per-packet
	// This allows dynamic config referencing other step outputs
	if step.Runtime != nil && len(step.Runtime.Raw) > 0 {
		if isStreaming {
			if err := validateTemplateScope(step.Runtime.Raw, streamingRuntimeScope()); err != nil {
				return nil, fmt.Errorf("step %q runtime has invalid templates: %w", getStepID(step), err)
			}
		}
		vars := buildRuntimeCELVars(payload, storyInputs, steps)
		return s.evaluateWithBlock(ctx, step.Runtime.Raw, vars)
	}

	// For batch steps or steps without runtime field, evaluate with block
	// This is the legacy/batch behavior
	if step.With != nil {
		if isStreaming {
			if err := validateTemplateScope(step.With.Raw, streamingStaticScope()); err != nil {
				return nil, fmt.Errorf("step %q with block not valid for streaming: %w", getStepID(step), err)
			}
			inputs := storyInputs
			if inputs == nil {
				inputs = map[string]any{}
			}
			vars := map[string]any{"inputs": inputs}
			return s.evaluateWithBlock(ctx, step.With.Raw, vars)
		}
		vars := buildCELVars(payload, storyInputs, steps)
		return s.evaluateWithBlock(ctx, step.With.Raw, vars)
	}

	return nil, nil
}

func validateTemplateScope(raw json.RawMessage, scope templating.ExpressionScope) error {
	if len(raw) == 0 {
		return nil
	}
	var node any
	if err := json.Unmarshal(raw, &node); err != nil {
		return fmt.Errorf("invalid template JSON: %w", err)
	}
	if m, ok := node.(map[string]any); ok {
		node = rewriteLegacyExpressions(m)
	}
	normalized, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to normalize template JSON: %w", err)
	}
	return templating.ValidateJSONTemplates(normalized, scope)
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
	inputs := storyInputs
	if inputs == nil {
		inputs = map[string]any{}
	}
	vars := map[string]any{
		"packet": payloadMap,       // Current packet data
		"inputs": inputs,           // Story inputs
		"steps":  map[string]any{}, // Always provide steps root to avoid nil index failures.
	}

	if len(stepsRaw) > 0 {
		if flattened := flattenStepOutputs(stepsRaw); len(flattened) > 0 {
			vars["steps"] = flattened // Flattened step outputs
		}
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

func outputsForJoin(stepVars map[string]any, stepID string, payload, inputs *structpb.Struct) map[string]any {
	if stepID != "" && stepVars != nil {
		if stepData, ok := stepVars[stepID].(map[string]any); ok {
			if outputs, ok := stepData["outputs"].(map[string]any); ok {
				return outputs
			}
		}
	}
	return mergeMaps(payloadAsMap(payload), payloadAsMap(inputs))
}

func (s *Server) validateProtocolMetadata(md metadata.MD) error {
	return coretransport.ValidateProtocolVersion(metadataValue(md, coretransport.ProtocolMetadataKey))
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

func metadataValue(md metadata.MD, key string) string {
	if len(md) == 0 || strings.TrimSpace(key) == "" {
		return ""
	}
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (s *Server) clampMaxDownstreams(policy routingPolicy) routingPolicy {
	if s == nil || s.maxDownstreamsHardCap <= 0 {
		return policy
	}
	if policy.maxDownstreams <= 0 || policy.maxDownstreams > s.maxDownstreamsHardCap {
		policy.maxDownstreams = s.maxDownstreamsHardCap
	}
	return policy
}

func findStepIndex(story *bubuv1alpha1.Story, stepID string) (int, error) {
	if story == nil {
		return -1, fmt.Errorf("story is nil")
	}
	for i := range story.Spec.Steps {
		if getStepID(&story.Spec.Steps[i]) == stepID {
			return i, nil
		}
	}
	return -1, fmt.Errorf("step %q not found in story", stepID)
}

func findNextDependentStep(story *bubuv1alpha1.Story, sourceStepID, transport string, startIndex int) (*bubuv1alpha1.Step, int) {
	if story == nil {
		return nil, -1
	}
	for i := startIndex + 1; i < len(story.Spec.Steps); i++ {
		step := &story.Spec.Steps[i]
		if !dependsOnStep(step, sourceStepID) {
			continue
		}
		if transport != "" && step.Transport != transport {
			continue
		}
		return step, i
	}
	return nil, -1
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
