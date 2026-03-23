package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/templating"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	metaMaterializeNextStep = "materialize-next-step"
	materializeModeObject   = "object"
)

type materializeRequest struct {
	Mode     string         `json:"mode"`
	Template any            `json:"template"`
	Vars     map[string]any `json:"vars"`
}

type materializeRequired struct {
	request materializeRequest
	reason  string
}

func (e *materializeRequired) Error() string {
	if e == nil {
		return "materialize required"
	}
	if e.reason == "" {
		return "materialize required"
	}
	return fmt.Sprintf("materialize required: %s", e.reason)
}

func (e *materializeRequired) Request() materializeRequest {
	if e == nil {
		return materializeRequest{}
	}
	return e.request
}

func (s *Server) shouldInjectOffloaded() bool {
	return strings.EqualFold(strings.TrimSpace(s.offloadedPolicy), "inject")
}

func materializeStepID(targetStepID string) string {
	return kubeutil.ComposeName("materialize", targetStepID)
}

func materializeStepRunName(storyRunName, stepID string) string {
	return kubeutil.ComposeName(storyRunName, stepID)
}

func materializeNextStep(metadata map[string]string) string {
	if metadata == nil {
		return ""
	}
	return strings.TrimSpace(metadata[metaMaterializeNextStep])
}

func materializeInputsStruct(req materializeRequest) (*structpb.Struct, error) {
	payload := map[string]any{
		"mode":     req.Mode,
		"template": req.Template,
		"vars":     req.Vars,
	}
	return structpb.NewStruct(payload)
}

func unwrapMaterializeResult(inputs *structpb.Struct) (*structpb.Struct, error) {
	if inputs == nil {
		return nil, fmt.Errorf("materialize result missing inputs")
	}
	payload := inputs.AsMap()
	if len(payload) == 0 {
		return nil, fmt.Errorf("materialize result missing payload")
	}
	// Check for explicit error from the materialize engram.
	if errVal, ok := payload["error"]; ok {
		return nil, fmt.Errorf("materialize engram returned error: %v", errVal)
	}
	result, ok := payload["result"]
	if !ok {
		// No "result" key — log a warning and pass through the raw payload.
		// This can happen if the materialize engram format changed.
		return inputs, nil
	}
	resultMap, ok := result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("materialize result must be object, got %T", result)
	}
	return structpb.NewStruct(resultMap)
}

func (s *Server) ensureMaterializeStepRun(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, targetStepID string) (string, error) {
	if storyRun == nil {
		return "", fmt.Errorf("storyrun is required for materialize")
	}
	if strings.TrimSpace(s.materializeEngram) == "" {
		return "", fmt.Errorf("materialize engram is not configured")
	}
	stepID := materializeStepID(targetStepID)
	stepRunName := materializeStepRunName(storyRun.Name, stepID)
	key := types.NamespacedName{Name: stepRunName, Namespace: storyRun.Namespace}

	var existing runsv1alpha1.StepRun
	if err := s.client.Get(ctx, key, &existing); err == nil {
		return stepID, nil
	} else if !apierrors.IsNotFound(err) {
		return "", err
	}

	labels := runsidentity.SelectorLabels(storyRun.Name)
	if story != nil {
		labels[contracts.StoryNameLabelKey] = story.Name
	}
	labels[contracts.MaterializeLabelKey] = "true"

	annotations := map[string]string{
		contracts.MaterializePurposeAnnotation: "runtime",
		contracts.MaterializeTargetAnnotation:  targetStepID,
		contracts.MaterializeModeAnnotation:    "deployment",
	}

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:        stepRunName,
			Namespace:   storyRun.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: storyRun.Name}},
			StepID:      stepID,
			EngramRef:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: s.materializeEngram}},
		},
	}

	if err := s.client.Create(ctx, stepRun); err != nil {
		return "", err
	}
	return stepID, nil
}

func (s *Server) handleMaterializeResult(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	nextStepID string,
	packet *transportpb.DataPacket,
) error {
	if nextStepID == "" {
		return nil
	}
	resolvedInputs, err := unwrapMaterializeResult(packet.GetInputs())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to unwrap materialize result: %v", err)
	}
	nextStep := findStepByID(story, nextStepID)
	if nextStep == nil {
		return status.Errorf(codes.NotFound, "materialize target step %q not found", nextStepID)
	}
	return s.routePacket(ctx, storyRun, story, nextStep, resolvedInputs, packet.Metadata[metaCurrentStepID], packet.GetPayload(), packet)
}

func findStepByID(story *bubuv1alpha1.Story, stepID string) *bubuv1alpha1.Step {
	if story == nil || stepID == "" {
		return nil
	}
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if getStepID(step) == stepID {
			return step
		}
	}
	return nil
}

func materializeRequestFromTemplate(template map[string]any, vars map[string]any, reason string) error {
	return &materializeRequired{
		request: materializeRequest{
			Mode:     materializeModeObject,
			Template: template,
			Vars:     vars,
		},
		reason: reason,
	}
}

func (s *Server) routeToMaterialize(
	ctx context.Context,
	storyRun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	targetStep *bubuv1alpha1.Step,
	payload *structpb.Struct,
	originalPacket *transportpb.DataPacket,
	req materializeRequest,
) error {
	if storyRun == nil || targetStep == nil {
		return nil
	}
	stepID, err := s.ensureMaterializeStepRun(ctx, storyRun, story, getStepID(targetStep))
	if err != nil {
		return err
	}
	inputs, err := materializeInputsStruct(req)
	if err != nil {
		return err
	}
	out := &transportpb.DataPacket{
		Metadata:   copyMetadataForStep(originalPacket.Metadata, storyRun.Name, storyRun.Namespace, stepID),
		Payload:    payload,
		Inputs:     inputs,
		Transports: cloneTransports(originalPacket.GetTransports()),
		Envelope:   cloneStreamEnvelope(originalPacket.GetEnvelope()),
		Audio:      cloneAudioFrame(originalPacket.GetAudio()),
		Video:      cloneVideoFrame(originalPacket.GetVideo()),
		Binary:     cloneBinaryFrame(originalPacket.GetBinary()),
	}
	if out.Metadata == nil {
		out.Metadata = map[string]string{}
	}
	out.Metadata[metaMaterializeNextStep] = getStepID(targetStep)
	if ok := s.streamManager.SendOrBufferWithPolicyAndLimits(ctx, storyRun.Name, storyRun.Namespace, stepID, out, nil, defaultBufferLimits()); !ok {
		return status.Errorf(codes.ResourceExhausted, "downstream buffer full for materialize step %q", stepID)
	}
	return nil
}

func (s *Server) buildMaterializeError(template map[string]any, vars map[string]any, reason string) error {
	return materializeRequestFromTemplate(template, vars, reason)
}

func (s *Server) materializeRequestFromRuntime(raw json.RawMessage, vars map[string]any, reason string) error {
	var withMap map[string]any
	if err := json.Unmarshal(raw, &withMap); err != nil {
		return fmt.Errorf("failed to unmarshal materialize template: %w", err)
	}
	return materializeRequestFromTemplate(withMap, vars, reason)
}

var stepOutputRefPattern = regexp.MustCompile(
	`steps\s*\.\s*([a-zA-Z0-9_\-]+)\s*\.` +
		`|steps\s*\[\s*['"]([a-zA-Z0-9_\-]+)['"]\s*\]\s*\.`,
)

func detectOffloadedOutputRefs(expr string, steps map[string]any) *templating.ErrOffloadedDataUsage {
	if expr == "" || len(steps) == 0 {
		return nil
	}
	matches := stepOutputRefPattern.FindAllStringSubmatch(expr, -1)
	for _, match := range matches {
		stepName := ""
		if len(match) > 1 && match[1] != "" {
			stepName = match[1]
		} else if len(match) > 2 && match[2] != "" {
			stepName = match[2]
		}
		if stepName == "" {
			continue
		}
		if stepOutputHasStorageRef(steps, stepName) {
			return &templating.ErrOffloadedDataUsage{
				Reason: fmt.Sprintf("template references offloaded output from step %q", stepName),
			}
		}
	}
	return nil
}

func stepOutputHasStorageRef(steps map[string]any, stepName string) bool {
	if steps == nil || stepName == "" {
		return false
	}
	stepCtx, ok := steps[stepName]
	if !ok || stepCtx == nil {
		return false
	}
	if containsStorageRef(stepCtx, 0) {
		return true
	}
	stepMap, ok := stepCtx.(map[string]any)
	if !ok {
		return false
	}
	if output, ok := stepMap["output"]; ok && containsStorageRef(output, 0) {
		return true
	}
	if outputs, ok := stepMap["outputs"]; ok && containsStorageRef(outputs, 0) {
		return true
	}
	return false
}

func containsStorageRef(value any, depth int) bool {
	if depth > 8 || value == nil {
		return false
	}
	switch v := value.(type) {
	case map[string]any:
		if _, ok := v[templating.StorageRefKey]; ok {
			return true
		}
		for _, entry := range v {
			if containsStorageRef(entry, depth+1) {
				return true
			}
		}
	case map[any]any:
		if _, ok := v[templating.StorageRefKey]; ok {
			return true
		}
		for _, entry := range v {
			if containsStorageRef(entry, depth+1) {
				return true
			}
		}
	case []any:
		for _, entry := range v {
			if containsStorageRef(entry, depth+1) {
				return true
			}
		}
	}
	return false
}
