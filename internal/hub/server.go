package hub

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	bobrapetcel "github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/refs"
	grpc_metrics "github.com/bubustack/bobravoz-grpc/pkg/metrics"
	hubv1 "github.com/bubustack/bobravoz-grpc/proto/v1"
	"github.com/go-logr/logr"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	metaStoryRunName  = "storyrun-name"
	metaStoryRunNS    = "storyrun-namespace"
	metaCurrentStepID = "current-step-id"
	trueString        = "true"
)

// Server is the gRPC hub server.
type Server struct {
	hubv1.UnimplementedHubServiceServer
	client            client.Client
	cache             *storyCache
	log               logr.Logger
	streamManager     *StreamManager
	perMessageTimeout time.Duration
}

// NewServer creates a new hub server.
func NewServer(k8sClient client.Client) *Server {
	cache := newStoryCache(k8sClient)
	return &Server{
		client:        k8sClient,
		cache:         cache,
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

	// Server options: message size limits, optional TLS/mTLS, optional keepalive
	var opts []grpc.ServerOption
	const defaultMax = 10 * 1024 * 1024
	recvMax := defaultMax
	if v := os.Getenv("BUBU_HUB_MAX_RECV_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			recvMax = n
		}
	}
	sendMax := defaultMax
	if v := os.Getenv("BUBU_HUB_MAX_SEND_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			sendMax = n
		}
	}
	// OpenTelemetry: prefer StatsHandler over deprecated interceptors
	opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	// Prometheus metrics interceptors + custom RPC metrics
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
	// TLS/mTLS is configurable. If TLS is not configured, default to plaintext unless
	// explicitly required via BUBU_HUB_REQUIRE_TLS=true. If REQUIRE_TLS is false and no
	// certs provided, run insecure (plaintext) with a warning.
	certFile := os.Getenv("BUBU_HUB_TLS_CERT_FILE")
	keyFile := os.Getenv("BUBU_HUB_TLS_KEY_FILE")
	caFile := os.Getenv("BUBU_HUB_CA_FILE") // optional: enable mTLS if provided
	if certFile != "" && keyFile != "" {
		// Load server cert/key
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("failed to load hub TLS keypair: %w", err)
		}
		tlsConf := &tls.Config{Certificates: []tls.Certificate{cert}}
		if caFile != "" {
			// Enable mutual TLS if CA is provided
			pem, err := os.ReadFile(caFile)
			if err != nil {
				return fmt.Errorf("failed to read hub CA file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return fmt.Errorf("failed to append hub CA certs from %s", caFile)
			}
			tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
			tlsConf.ClientCAs = pool
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConf)))
	} else {
		if os.Getenv("BUBU_HUB_REQUIRE_TLS") == trueString {
			return fmt.Errorf("hub requires TLS (BUBU_HUB_REQUIRE_TLS=true) but no certs provided; set BUBU_HUB_TLS_CERT_FILE and BUBU_HUB_TLS_KEY_FILE")
		}
		s.log.Info("Starting Hub without TLS (plaintext mode)")
	}

	// Optional keepalive (server-side). Disabled unless explicitly set.
	var kaParams keepalive.ServerParameters
	var haveParams bool
	if v := os.Getenv("BUBU_HUB_KEEPALIVE_TIME"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			kaParams.Time = d
			haveParams = true
		}
	}
	if v := os.Getenv("BUBU_HUB_KEEPALIVE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			kaParams.Timeout = d
			haveParams = true
		}
	}
	if haveParams {
		opts = append(opts, grpc.KeepaliveParams(kaParams))
	}
	// Always apply message size limits
	opts = append(opts, grpc.MaxRecvMsgSize(recvMax), grpc.MaxSendMsgSize(sendMax))

	// Per-message (stream-wide) timeout. If unset, enforce a sensible default to avoid indefinite hangs.
	if v := os.Getenv("BUBU_HUB_PER_MESSAGE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			s.perMessageTimeout = d
		}
	} else {
		s.perMessageTimeout = 30 * time.Second
		s.log.Info("BUBU_HUB_PER_MESSAGE_TIMEOUT not set; using default", "timeout", s.perMessageTimeout)
	}

	grpcServer := grpc.NewServer(opts...)
	hubv1.RegisterHubServiceServer(grpcServer, s)

	// Enable latency histograms and register gRPC metrics server
	grpc_prometheus.EnableHandlingTimeHistogram()
	grpc_prometheus.Register(grpcServer)

	// Start background evictor to cap buffer cardinality over time
	s.streamManager.StartEvictor(ctx)

	// Start a heartbeat sender to keep downstream connections alive.
	s.startHeartbeatSender(ctx)

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

// isHeartbeat checks if a DataPacket is a heartbeat message.
func isHeartbeat(packet *hubv1.DataPacket) bool {
	return packet != nil && packet.Metadata != nil && packet.Metadata["bubu-heartbeat"] == trueString
}

func (s *Server) startHeartbeatSender(ctx context.Context) {
	interval := 10 * time.Second // Default interval, matches SDK
	if v := os.Getenv("BUBU_HUB_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			interval = d
		}
	}

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
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
func (s *Server) Process(stream hubv1.HubService_ProcessServer) error {
	s.log.Info("New stream established")
	ctx := stream.Context()
	// Apply a stream-wide deadline if configured via BUBU_HUB_PER_MESSAGE_TIMEOUT
	if s.perMessageTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.perMessageTimeout)
		defer cancel()
	}
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

	// Handle incoming messages in this goroutine and return when the stream ends
	if err := s.messageLoop(ctx, stream, storyRunName, storyRunNS, currentStepID); err != nil {
		return err
	}
	s.log.Info("Stream ended", "storyRun", storyRunName, "step", currentStepID)
	return nil
}

func (s *Server) messageLoop(ctx context.Context, stream hubv1.HubService_ProcessServer, storyRunName, storyRunNS, currentStepID string) error {
	for {
		// Before blocking on Recv, check if the context has been canceled.
		select {
		case <-ctx.Done():
			s.log.Info("Stream context done, closing message loop", "storyRun", storyRunName, "step", currentStepID)
			return ctx.Err()
		default:
			// Non-blocking check, proceed to Recv.
		}

		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				s.log.Info("Upstream closed the stream", "storyRun", storyRunName, "step", currentStepID)
				return nil // Clean exit.
			}
			s.log.Error(err, "Error receiving from stream", "storyRun", storyRunName, "step", currentStepID)
			return err
		}

		in := req.GetPacket()
		if in == nil {
			continue // ignore empty wrapper
		}

		// Record received message (exclude heartbeats to avoid skewing throughput)
		if !isHeartbeat(in) {
			grpc_metrics.RecordHubMessageReceived(storyRunName, currentStepID)
		}

		// Get the StoryRun and Story to find the next step, with retries for transient errors.
		var storyRun *runsv1alpha1.StoryRun
		var story *bubuv1alpha1.Story
		var cacheErr error
		for i := 0; i < 3; i++ {
			storyRun, story, cacheErr = s.cache.Get(ctx, storyRunName, storyRunNS)
			if cacheErr == nil {
				break
			}
			s.log.V(1).Info("Retrying to get story and run from cache", "storyRun", storyRunName, "attempt", i+1)
			time.Sleep(100 * time.Millisecond)
		}
		if cacheErr != nil {
			s.log.Error(cacheErr, "Failed to get story and run after retries", "storyRun", storyRunName)
			return status.Errorf(codes.Unavailable, "failed to get story backend data: %v", cacheErr)
		}

		nextStep, nextEngramStep, err := findNextSteps(story, currentStepID)
		if err != nil {
			if strings.Contains(err.Error(), "last step") {
				s.log.Info("End of pipeline reached for packet", "storyRun", storyRunName, "lastStep", currentStepID)
			} else {
				s.log.Error(err, "Could not determine next step", "storyRun", storyRunName, "currentStep", currentStepID)
			}
			continue
		}

		// If it's a heartbeat, forward it to the next engram step to keep the downstream connection alive.
		if isHeartbeat(in) {
			s.log.V(1).Info("Forwarding heartbeat", "from", currentStepID, "storyRun", storyRunName)
			if nextEngramStep != nil {
				nextEngramStepID := getStepID(nextEngramStep)
				if ok := s.streamManager.SendOrBuffer(ctx, storyRun.Name, nextEngramStepID, in); !ok {
					s.log.Error(nil, "Failed to deliver or buffer heartbeat; dropping", "downstreamStep", nextEngramStepID)
				}
			}
			continue
		}

		s.log.Info("Received packet", "storyRun", storyRunName, "fromStep", currentStepID)

		// This is where we will handle primitive steps.
		processedPayload := in.Payload
		if nextStep.Ref == nil { // It's a primitive
			processedPayload, err = s.evaluatePrimitive(ctx, nextStep, in.Payload)
			if err != nil {
				s.log.Error(err, "Failed to evaluate primitive", "step", getStepID(nextStep))
				return status.Errorf(codes.Internal, "failed to evaluate primitive step %q: %v", getStepID(nextStep), err)
			}
		}

		// If there's no subsequent engram, we're done with this branch of the pipeline.
		if nextEngramStep == nil {
			s.log.Info("End of engram chain for this packet.", "storyRun", storyRunName, "lastStep", currentStepID)
			continue
		}

		// Determine the execution mode of the next engram. If it's batch (job), create a StepRun.
		if nextEngramStep.Ref != nil {
			var nextEngram bubuv1alpha1.Engram
			key := types.NamespacedName{Namespace: storyRunNS, Name: nextEngramStep.Ref.Name}
			if err := s.client.Get(ctx, key, &nextEngram); err == nil {
				if nextEngram.Spec.Mode == enums.WorkloadModeJob {
					if err := s.createBatchStepRun(ctx, storyRun, story, nextEngramStep, in.Inputs, currentStepID); err != nil {
						s.log.Error(err, "Failed to create StepRun for batch engram", "step", getStepID(nextEngramStep))
						return status.Errorf(codes.Internal, "failed to create StepRun for batch step: %v", err)
					}
					// Do not forward to streaming; the batch StepRun will execute independently.
					continue
				}
			}
		}

		// Find the stream for the next engram step and forward the packet.
		nextEngramStepID := getStepID(nextEngramStep)
		out := &hubv1.DataPacket{
			Metadata: in.Metadata,
			Payload:  processedPayload,
			Inputs:   in.Inputs,
		}
		if ok := s.streamManager.SendOrBuffer(ctx, storyRun.Name, nextEngramStepID, out); !ok {
			s.log.Error(nil, "Failed to deliver or buffer packet; dropping and closing stream", "downstreamStep", nextEngramStepID)
			return status.Errorf(codes.ResourceExhausted, "downstream buffer full for step %q", nextEngramStepID)
		}
	}
}

// createBatchStepRun creates a StepRun for a batch (job-mode) engram step.
// If inputs is provided in the incoming packet, it is treated as pre-resolved and passed through.
func (s *Server) createBatchStepRun(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, inputs *structpb.Struct, upstreamStepID string) error {
	stepID := getStepID(step)
	name := fmt.Sprintf("%s-%s-%d", storyRun.Name, stepID, time.Now().UnixMilli())

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
			return fmt.Errorf("failed to marshal packet inputs: %w", err)
		}
		// Prevent oversized objects in the API server to avoid etcd bloat and create failures.
		maxInputs := 1 * 1024 * 1024
		if v := os.Getenv("BUBU_HUB_INPUTS_MAX_BYTES"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				maxInputs = n
			}
		}
		if len(b) > maxInputs {
			return fmt.Errorf("packet inputs size %d exceeds limit %d", len(b), maxInputs)
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

func (s *Server) evaluatePrimitive(ctx context.Context, step *bubuv1alpha1.Step, payload *structpb.Struct) (*structpb.Struct, error) {
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

	var withMap map[string]any
	if err := json.Unmarshal(step.With.Raw, &withMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal 'with' block: %w", err)
	}

	// The input for the CEL expression will be the payload of the data packet.
	// Provide both 'payload' and 'inputs' for backward/forward compatibility across CEL env versions.
	inputVars := map[string]any{
		"payload": payload.AsMap(),
		"inputs":  payload.AsMap(),
	}

	evaluator, err := bobrapetcel.New(logging.NewCELLogger(s.log))
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL evaluator: %w", err)
	}
	// Rewrite legacy expressions that reference 'payload.' to 'inputs.' to support older CEL envs
	rewritten := make(map[string]any, len(withMap))
	for k, v := range withMap {
		if s, ok := v.(string); ok {
			if strings.HasPrefix(s, "{{") && strings.HasSuffix(s, "}}") {
				expr := strings.TrimSpace(s[2 : len(s)-2])
				if strings.Contains(expr, "payload.") {
					expr = strings.ReplaceAll(expr, "payload.", "inputs.")
					s = "{{ " + expr + " }}"
				}
			}
			rewritten[k] = s
			continue
		}
		rewritten[k] = v
	}

	resultMap, err := evaluator.ResolveWithInputs(ctx, rewritten, inputVars)
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
