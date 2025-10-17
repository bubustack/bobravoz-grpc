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

	var opts []grpc.ServerOption
	opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
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

	if tlsOpt, err := tlsServerOptionFromEnv(s.log); err != nil {
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
		in, err := s.recvFromStream(ctx, stream, storyRunName, currentStepID)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if in == nil { // ignore empty wrapper
			continue
		}

		if err := s.processPacket(ctx, storyRunName, storyRunNS, currentStepID, in); err != nil {
			return err
		}
	}
}

// recvFromStream receives the next packet from the stream, handling context
// cancellation and EOF semantics consistently.
func (s *Server) recvFromStream(ctx context.Context, stream hubv1.HubService_ProcessServer, storyRunName, currentStepID string) (*hubv1.DataPacket, error) {
	select {
	case <-ctx.Done():
		s.log.Info("Stream context done, closing message loop", "storyRun", storyRunName, "step", currentStepID)
		return nil, ctx.Err()
	default:
	}

	req, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			s.log.Info("Upstream closed the stream", "storyRun", storyRunName, "step", currentStepID)
			return nil, io.EOF
		}
		s.log.Error(err, "Error receiving from stream", "storyRun", storyRunName, "step", currentStepID)
		return nil, err
	}

	in := req.GetPacket()
	if in == nil {
		return nil, nil
	}
	if !isHeartbeat(in) {
		grpc_metrics.RecordHubMessageReceived(storyRunName, currentStepID)
	}
	return in, nil
}

// processPacket executes the routing logic for a received packet.
func (s *Server) processPacket(ctx context.Context, storyRunName, storyRunNS, currentStepID string, in *hubv1.DataPacket) error {
	storyRun, story, err := s.getStoryAndRunWithRetry(ctx, storyRunName, storyRunNS)
	if err != nil {
		s.log.Error(err, "Failed to get story and run after retries", "storyRun", storyRunName)
		return status.Errorf(codes.Unavailable, "failed to get story backend data: %v", err)
	}

	nextStep, nextEngramStep, err := findNextSteps(story, currentStepID)
	if err != nil {
		if strings.Contains(err.Error(), "last step") {
			s.log.Info("End of pipeline reached for packet", "storyRun", storyRunName, "lastStep", currentStepID)
		} else {
			s.log.Error(err, "Could not determine next step", "storyRun", storyRunName, "currentStep", currentStepID)
		}
		return nil
	}

	if isHeartbeat(in) {
		s.log.V(1).Info("Forwarding heartbeat", "from", currentStepID, "storyRun", storyRunName)
		if nextEngramStep != nil {
			nextEngramStepID := getStepID(nextEngramStep)
			if ok := s.streamManager.SendOrBuffer(ctx, storyRun.Name, nextEngramStepID, in); !ok {
				s.log.Error(nil, "Failed to deliver or buffer heartbeat; dropping", "downstreamStep", nextEngramStepID)
			}
		}
		return nil
	}

	s.log.Info("Received packet", "storyRun", storyRunName, "fromStep", currentStepID)

	processedPayload := in.Payload
	if nextStep.Ref == nil {
		processedPayload, err = s.evaluatePrimitive(ctx, nextStep, in.Payload)
		if err != nil {
			s.log.Error(err, "Failed to evaluate primitive", "step", getStepID(nextStep))
			return status.Errorf(codes.Internal, "failed to evaluate primitive step %q: %v", getStepID(nextStep), err)
		}
	}

	if nextEngramStep == nil {
		s.log.Info("End of engram chain for this packet.", "storyRun", storyRunName, "lastStep", currentStepID)
		return nil
	}

	if nextEngramStep.Ref != nil {
		var nextEngram bubuv1alpha1.Engram
		key := types.NamespacedName{Namespace: storyRunNS, Name: nextEngramStep.Ref.Name}
		if err := s.client.Get(ctx, key, &nextEngram); err == nil {
			if nextEngram.Spec.Mode == enums.WorkloadModeJob {
				if err := s.createBatchStepRun(ctx, storyRun, story, nextEngramStep, in.Inputs, currentStepID); err != nil {
					s.log.Error(err, "Failed to create StepRun for batch engram", "step", getStepID(nextEngramStep))
					return status.Errorf(codes.Internal, "failed to create StepRun for batch step: %v", err)
				}
				return nil
			}
		}
	}

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
	return nil
}

// getStoryAndRunWithRetry fetches StoryRun and Story with simple retries for transient errors.
func (s *Server) getStoryAndRunWithRetry(ctx context.Context, storyRunName, storyRunNS string) (*runsv1alpha1.StoryRun, *bubuv1alpha1.Story, error) {
	var (
		storyRun *runsv1alpha1.StoryRun
		story    *bubuv1alpha1.Story
		err      error
	)
	for i := 0; i < 3; i++ {
		storyRun, story, err = s.cache.Get(ctx, storyRunName, storyRunNS)
		if err == nil {
			return storyRun, story, nil
		}
		s.log.V(1).Info("Retrying to get story and run from cache", "storyRun", storyRunName, "attempt", i+1)
		time.Sleep(100 * time.Millisecond)
	}
	return nil, nil, err
}

// parseMaxMsgSizesFromEnv returns the recv and send message size limits.
func parseMaxMsgSizesFromEnv() (int, int) {
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
	return recvMax, sendMax
}

// parsePerMessageTimeoutFromEnv parses stream-wide per-message timeout or returns a default.
func parsePerMessageTimeoutFromEnv(logger logr.Logger) time.Duration {
	if v := os.Getenv("BUBU_HUB_PER_MESSAGE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	d := 30 * time.Second
	logger.Info("BUBU_HUB_PER_MESSAGE_TIMEOUT not set; using default", "timeout", d)
	return d
}

// tlsServerOptionFromEnv builds a TLS credentials option from environment variables.
func tlsServerOptionFromEnv(logger logr.Logger) (grpc.ServerOption, error) {
	certFile := os.Getenv("BUBU_HUB_TLS_CERT_FILE")
	keyFile := os.Getenv("BUBU_HUB_TLS_KEY_FILE")
	caFile := os.Getenv("BUBU_HUB_CA_FILE")
	// If any TLS-related env is set, we must not silently start plaintext.
	if certFile != "" || keyFile != "" || caFile != "" || os.Getenv("BUBU_HUB_REQUIRE_TLS") == trueString {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("hub TLS requested via env but cert/key missing; set BUBU_HUB_TLS_CERT_FILE and BUBU_HUB_TLS_KEY_FILE")
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
	logger.Info("Starting Hub without TLS (plaintext mode)")
	return nil, nil
}

// serverKeepaliveOptionFromEnv builds an optional keepalive server option.
func serverKeepaliveOptionFromEnv() (grpc.ServerOption, bool) {
	var kaParams keepalive.ServerParameters
	var have bool
	if v := os.Getenv("BUBU_HUB_KEEPALIVE_TIME"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			kaParams.Time = d
			have = true
		}
	}
	if v := os.Getenv("BUBU_HUB_KEEPALIVE_TIMEOUT"); v != "" {
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
