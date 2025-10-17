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

package transport

import (
	"context"
	"fmt"
	"os"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// GRPCTransport reconciles the transport for streaming StoryRuns using gRPC.
type GRPCTransport struct {
	client.Client
	Log logr.Logger
}

// NewGRPCTransport creates a new GRPCTransport.
func NewGRPCTransport(cli client.Client) *GRPCTransport {
	return &GRPCTransport{
		Client: cli,
		Log:    log.Log.WithName("grpc-transport"),
	}
}

// Reconcile configures the gRPC connections between engrams in a streaming story.
func (r *GRPCTransport) Reconcile(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) error {
	logger := log.FromContext(ctx).WithName("grpc-transport")
	logger.Info("Reconciling gRPC transport for StoryRun")

	hubService := getHubServiceDNS(storyRun.Namespace)
	var lastEngramStep *bubuv1alpha1.Step

	for i, currentStep := range story.Spec.Steps {
		// We only care about steps that are engrams.
		if currentStep.Ref == nil {
			continue
		}

		// If this is the first engram step, there's no upstream connection to make yet.
		if lastEngramStep == nil {
			lastEngramStep = &story.Spec.Steps[i]
			// Clear any previous upstream host setting.
			engramName := getEngramNameForStep(story, storyRun, lastEngramStep)
			if err := r.configureEngram(ctx, storyRun.Namespace, engramName, "", ""); err != nil {
				logger.Error(err, "Failed to clear initial upstream for engram", "engram", engramName)
				return err
			}
			continue
		}

		// We have a pair of engrams: lastEngramStep and currentStep.
		// Check for intermediate primitive steps that require the hub.
		connectionType := getConnectionType(story, lastEngramStep, &currentStep)

		var upstreamHost, downstreamHost string
		if connectionType == ConnectionTypeHubAndSpoke {
			downstreamHost = hubService
			upstreamHost = hubService
		} else { // P2P
			downstreamName := getEngramNameForStep(story, storyRun, &currentStep)
			downstreamHost = getServiceDNS(storyRun.Namespace, downstreamName)
			upstreamName := getEngramNameForStep(story, storyRun, lastEngramStep)
			upstreamHost = getServiceDNS(storyRun.Namespace, upstreamName)
		}

		// Configure the downstream of the last engram.
		lastEngramName := getEngramNameForStep(story, storyRun, lastEngramStep)
		logger.Info("Configuring downstream for engram", "engram", lastEngramName, "downstream", downstreamHost)
		if err := r.configureEngram(ctx, storyRun.Namespace, lastEngramName, "", downstreamHost); err != nil {
			logger.Error(err, "Failed to configure downstream for engram", "engram", lastEngramName)
			return err
		}

		// Configure the upstream of the current engram.
		currentEngramName := getEngramNameForStep(story, storyRun, &currentStep)
		logger.Info("Configuring upstream for engram", "engram", currentEngramName, "upstream", upstreamHost)
		if err := r.configureEngram(ctx, storyRun.Namespace, currentEngramName, upstreamHost, ""); err != nil {
			logger.Error(err, "Failed to configure upstream for engram", "engram", currentEngramName)
			return err
		}

		lastEngramStep = &story.Spec.Steps[i]
	}

	// The last engram in the chain has no downstream peer.
	if lastEngramStep != nil {
		engramName := getEngramNameForStep(story, storyRun, lastEngramStep)
		logger.Info("Clearing downstream for final engram in chain", "engram", engramName)
		if err := r.configureEngram(ctx, storyRun.Namespace, engramName, "", ""); err != nil {
			logger.Error(err, "Failed to clear final downstream for engram", "engram", engramName)
			return err
		}
	}

	return nil
}

// ConnectionType defines the type of connection between two engrams.
type ConnectionType string

const (
	// ConnectionTypeP2P means engrams connect directly to each other.
	ConnectionTypeP2P ConnectionType = "PeerToPeer"
	// ConnectionTypeHubAndSpoke means engrams connect via the bobravoz hub.
	ConnectionTypeHubAndSpoke ConnectionType = "HubAndSpoke"
)

func getConnectionType(story *bubuv1alpha1.Story, upstream, downstream *bubuv1alpha1.Step) ConnectionType {
	isHubPrimitive := func(stepType enums.StepType) bool {
		switch stepType {
		case enums.StepTypeTransform, enums.StepTypeFilter, enums.StepTypeSetData, enums.StepTypeMergeData, enums.StepTypeCondition:
			return true
		default:
			return false
		}
	}

	startIndex := -1
	endIndex := -1
	for i, step := range story.Spec.Steps {
		if &step == upstream {
			startIndex = i
		}
		if &step == downstream {
			endIndex = i
			break
		}
	}

	if startIndex != -1 && endIndex != -1 {
		for i := startIndex + 1; i < endIndex; i++ {
			if isHubPrimitive(story.Spec.Steps[i].Type) {
				return ConnectionTypeHubAndSpoke
			}
		}
	}

	return ConnectionTypeP2P
}

func getEngramNameForStep(story *bubuv1alpha1.Story, storyRun *runsv1alpha1.StoryRun, step *bubuv1alpha1.Step) string {
	if story.Spec.StreamingStrategy == enums.StreamingStrategyPerStoryRun {
		return fmt.Sprintf("%s-%s", storyRun.Name, step.Name)
	}
	// Default is PerStory
	return step.Ref.Name
}

func getHubServiceDNS(namespace string) string {
	// This assumes the hub service is named 'bobravoz-grpc-hub' and is in the same namespace as the operator.
	// In a real-world scenario, this would be configurable.
	return fmt.Sprintf("bobravoz-grpc-hub.%s.svc.cluster.local", namespace)
}

func (r *GRPCTransport) configureEngram(ctx context.Context, namespace, engramName, upstreamHost, downstreamHost string) error {
	logger := r.Log.WithName("grpc-transport").WithValues("namespace", namespace, "engram", engramName)

	// Fetch the Deployment for the engram
	var deployment appsv1.Deployment
	key := types.NamespacedName{Namespace: namespace, Name: engramName}
	if err := r.Get(ctx, key, &deployment); err != nil {
		logger.Error(err, "Failed to get Deployment for engram")
		return client.IgnoreNotFound(err)
	}

	// Create a patch to update environment variables
	original := deployment.DeepCopy()
	needsPatch := false
	for i := range deployment.Spec.Template.Spec.Containers {
		container := &deployment.Spec.Template.Spec.Containers[i]
		// Update or remove UPSTREAM_HOST based on provided value
		if upstreamHost != "" {
			if updateEnvVar(&container.Env, "UPSTREAM_HOST", upstreamHost) {
				needsPatch = true
			}
		} else {
			if removeEnvVar(&container.Env, "UPSTREAM_HOST") {
				needsPatch = true
			}
		}
		// Update or remove DOWNSTREAM_HOST based on provided value
		if downstreamHost != "" {
			if updateEnvVar(&container.Env, "DOWNSTREAM_HOST", downstreamHost) {
				needsPatch = true
			}
		} else {
			if removeEnvVar(&container.Env, "DOWNSTREAM_HOST") {
				needsPatch = true
			}
		}

		// Optionally inject TLS client settings from a provisioned Secret (e.g., cert-manager)
		// Only when Hub requires TLS to avoid breaking plaintext deployments
		if os.Getenv("BUBU_HUB_REQUIRE_TLS") == "true" {
			if injectClientTLSIfConfigured(&deployment.Spec.Template.Spec, container) {
				needsPatch = true
			}
		}
	}

	if needsPatch {
		logger.Info("Patching Deployment with transport configuration", "upstream", upstreamHost, "downstream", downstreamHost)
		if err := r.Patch(ctx, &deployment, client.MergeFrom(original)); err != nil {
			logger.Error(err, "Failed to patch Deployment")
			return err
		}
	} else {
		logger.Info("Deployment already has correct transport configuration")
	}

	return nil
}

// getEngramSteps filters the story steps to only include those that reference an Engram.
func getEngramSteps(story *bubuv1alpha1.Story) []bubuv1alpha1.Step {
	var engramSteps []bubuv1alpha1.Step
	for _, step := range story.Spec.Steps {
		if step.Ref != nil {
			engramSteps = append(engramSteps, step)
		}
	}
	return engramSteps
}

// getServiceDNS returns the fully qualified domain name for a service.
func getServiceDNS(namespace, serviceName string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, namespace)
}

// updateEnvVar updates an environment variable in a list of env vars.
// Returns true if an update was made, false otherwise.
func updateEnvVar(env *[]corev1.EnvVar, name, value string) bool {
	for i, v := range *env {
		if v.Name == name {
			if v.Value == value {
				return false // Already up-to-date
			}
			(*env)[i].Value = value
			return true
		}
	}
	// If the variable doesn't exist, add it
	*env = append(*env, corev1.EnvVar{Name: name, Value: value})
	return true
}

// injectClientTLSIfConfigured mounts a known TLS Secret and injects SDK client TLS envs
// following community best practices (cert-manager-provisioned secret). No-op if
// the Secret isn't present or already mounted.
func injectClientTLSIfConfigured(podSpec *corev1.PodSpec, container *corev1.Container) bool {
	changed := false
	// Source secret name via env or default convention
	tlsSecretName := os.Getenv("BUBU_GRPC_CLIENT_TLS_SECRET_NAME")
	if tlsSecretName == "" {
		tlsSecretName = "engram-tls" // convention; can be overridden by env
	}

	// Mount path (read-only)
	mountPath := "/var/run/tls"

	// Ensure volume exists
	volName := "engram-tls"
	hasVolume := false
	for _, v := range podSpec.Volumes {
		if v.Name == volName {
			hasVolume = true
			break
		}
	}
	if !hasVolume {
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: tlsSecretName, Optional: func(b bool) *bool { return &b }(true)},
			},
		})
		changed = true
	}

	// Ensure container mount exists
	hasMount := false
	for _, m := range container.VolumeMounts {
		if m.Name == volName && m.MountPath == mountPath {
			hasMount = true
			break
		}
	}
	if !hasMount {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      volName,
			MountPath: mountPath,
			ReadOnly:  true,
		})
		changed = true
	}

	// Inject SDK client TLS envs (paths aligned with mount)
	if updateEnvVar(&container.Env, "BUBU_GRPC_CLIENT_TLS", "true") {
		changed = true
	}
	if updateEnvVar(&container.Env, "BUBU_GRPC_CA_FILE", mountPath+"/ca.crt") {
		changed = true
	}
	// Client cert/key for mTLS if server requires it
	if updateEnvVar(&container.Env, "BUBU_GRPC_CLIENT_CERT_FILE", mountPath+"/tls.crt") {
		changed = true
	}
	if updateEnvVar(&container.Env, "BUBU_GRPC_CLIENT_KEY_FILE", mountPath+"/tls.key") {
		changed = true
	}
	// Honor require-TLS enforcement on client side
	if updateEnvVar(&container.Env, "BUBU_GRPC_REQUIRE_TLS", "true") {
		changed = true
	}

	return changed
}

func (r *GRPCTransport) EnsureCleanUp(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) error {
	logger := log.FromContext(ctx).WithName("grpc-transport")
	logger.Info("gRPC transport cleanup for StoryRun")

	// Cleanup is only needed for the PerStory strategy.
	// For PerStoryRun, the engrams and their deployments are owned by the StoryRun
	// and will be garbage collected automatically when the StoryRun is deleted.
	if story.Spec.StreamingStrategy == enums.StreamingStrategyPerStoryRun {
		logger.Info("Skipping transport cleanup for PerStoryRun strategy")
		return nil
	}

	engramSteps := getEngramSteps(story)
	for _, step := range engramSteps {
		if step.Ref == nil {
			continue
		}
		engramName := getEngramNameForStep(story, storyRun, &step)
		if err := r.cleanupEngram(ctx, storyRun.Namespace, engramName); err != nil {
			// Log the error but continue trying to clean up other engrams
			logger.Error(err, "Failed to cleanup engram", "engram", engramName)
		}
	}

	return nil
}

func (r *GRPCTransport) cleanupEngram(ctx context.Context, namespace, engramName string) error {
	logger := r.Log.WithName("grpc-transport").WithValues("namespace", namespace, "engram", engramName)

	var deployment appsv1.Deployment
	key := types.NamespacedName{Namespace: namespace, Name: engramName}
	if err := r.Get(ctx, key, &deployment); err != nil {
		if client.IgnoreNotFound(err) == nil {
			logger.Info("Deployment not found, skipping cleanup")
			return nil
		}
		logger.Error(err, "Failed to get Deployment for engram cleanup")
		return err
	}

	original := deployment.DeepCopy()
	needsPatch := false
	for i := range deployment.Spec.Template.Spec.Containers {
		container := &deployment.Spec.Template.Spec.Containers[i]
		if removeEnvVar(&container.Env, "UPSTREAM_HOST") {
			needsPatch = true
		}
		if removeEnvVar(&container.Env, "DOWNSTREAM_HOST") {
			needsPatch = true
		}
	}

	if needsPatch {
		logger.Info("Patching Deployment to remove transport configuration")
		if err := r.Patch(ctx, &deployment, client.MergeFrom(original)); err != nil {
			logger.Error(err, "Failed to patch Deployment for cleanup")
			return err
		}
	} else {
		logger.Info("Deployment already clean of transport configuration")
	}

	return nil
}

// removeEnvVar removes an environment variable from a list of env vars.
// Returns true if a variable was removed, false otherwise.
func removeEnvVar(env *[]corev1.EnvVar, name string) bool {
	for i, v := range *env {
		if v.Name == name {
			*env = append((*env)[:i], (*env)[i+1:]...)
			return true
		}
	}
	return false
}
