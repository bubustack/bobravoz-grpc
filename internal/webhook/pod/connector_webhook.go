package pod

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	catalogv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	transportutil "github.com/bubustack/bobrapet/pkg/transport"
	"github.com/bubustack/bobrapet/pkg/transport/bindinginfo"
	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	connectorInjectedAnnotation = "transport.bobravoz.bubustack.io/connector-injected"
	connectorInjectedLabel      = "transport.bobravoz.bubustack.io/connector"
	injectedLabelValue          = "true"
	engramLabel                 = "bubustack.io/engram"
	storyLabel                  = contracts.StoryLabelKey
	stepLabel                   = contracts.StepLabelKey
	defaultGRPCPort             = "50051"
	storyAnnotationKey          = storyLabel
	stepAnnotationKey           = stepLabel
	storyRunAnnotationKey       = contracts.StoryRunAnnotation
)

var connectorWebhookLog = ctrl.Log.WithName("connector-webhook")

// ConnectorWebhook injects the transport connector sidecar into realtime Engram pods.
type ConnectorWebhook struct {
	client     client.Client
	image      string
	pullPolicy corev1.PullPolicy
}

// NewConnectorWebhook returns a configured ConnectorWebhook.
func NewConnectorWebhook(cli client.Client, image string, pullPolicy corev1.PullPolicy) *ConnectorWebhook {
	return &ConnectorWebhook{
		client:     cli,
		image:      image,
		pullPolicy: pullPolicy,
	}
}

// Handle mutates the incoming Pod if it represents a realtime Engram workload.
func (w *ConnectorWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	if req.Operation != admissionv1.Create {
		return admission.Allowed("only mutate Pod creations")
	}

	if w.image == "" {
		return admission.Allowed("connector image is not configured")
	}

	pod := &corev1.Pod{}
	if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if pod.Annotations != nil && pod.Annotations[connectorInjectedAnnotation] == injectedLabelValue {
		return admission.Allowed("connector already injected")
	}
	engramName := pod.Labels[engramLabel]
	if engramName == "" {
		return admission.Allowed("not an engram pod")
	}

	engram := &catalogv1alpha1.Engram{}
	if err := w.client.Get(ctx, types.NamespacedName{Name: engramName, Namespace: pod.Namespace}, engram); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("fetch engram %s/%s: %w", pod.Namespace, engramName, err))
	}

	bindingValue, err := w.resolveBinding(ctx, pod, engram)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if bindingValue == "" {
		return admission.Allowed("engram not bound to transport")
	}

	mutated := pod.DeepCopy()
	if mutated.Labels == nil {
		mutated.Labels = make(map[string]string)
	}
	if mutated.Annotations == nil {
		mutated.Annotations = make(map[string]string)
	}

	if err := w.injectConnector(mutated, engram, bindingValue); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	mutated.Annotations[contracts.TransportBindingAnnotation] = bindingValue
	mutated.Annotations[connectorInjectedAnnotation] = injectedLabelValue
	marshaled, err := json.Marshal(mutated)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}

// ConnectorInjectedLabel exposes the label applied to pods that received a connector sidecar.
func ConnectorInjectedLabel() string {
	return connectorInjectedLabel
}

// SetupWithManager registers the webhook with the controller-runtime server.
func (w *ConnectorWebhook) SetupWithManager(mgr ctrl.Manager) error {
	mgr.GetWebhookServer().Register("/mutate-transport-connector-pod", &admission.Webhook{Handler: w})
	return nil
}

// resolveBinding determines the transport binding value for the pod, checking
// pod annotations first, then falling back to the engram's annotation.
func (w *ConnectorWebhook) resolveBinding(ctx context.Context, pod *corev1.Pod, engram *catalogv1alpha1.Engram) (string, error) {
	if pod.Annotations != nil {
		if v := strings.TrimSpace(pod.Annotations[contracts.TransportBindingAnnotation]); v != "" {
			return v, nil
		}
	}
	ref := engram.Annotations[contracts.TransportBindingAnnotation]
	return w.resolveBindingEnvValue(ctx, pod.Namespace, ref)
}

func (w *ConnectorWebhook) injectConnector(pod *corev1.Pod, engram *catalogv1alpha1.Engram, bindingValue string) error {
	if len(pod.Spec.Containers) == 0 {
		connectorWebhookLog.Info("pod has no containers, skipping connector injection",
			"pod", pod.Name, "namespace", pod.Namespace)
		return nil
	}
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[connectorInjectedLabel] = injectedLabelValue

	main := &pod.Spec.Containers[0]
	bindingInfo, err := decodeBindingInfo(bindingValue)
	if err != nil {
		return fmt.Errorf("inject connector: %w", err)
	}

	// Get routing information from TransportBinding
	routingInfo := w.extractRoutingInfo(pod)

	env := w.buildEnv(pod, engram, bindingValue, bindingInfo, routingInfo)
	connector := corev1.Container{
		Name:            "transport-connector",
		Image:           w.image,
		ImagePullPolicy: w.pullPolicy,
		Command:         []string{"/connector"},
		Env:             env,
		VolumeMounts:    w.copyTLSVolumeMounts(pod),
		SecurityContext: deriveConnectorSecurityContext(main),
	}

	pod.Spec.Containers = append(pod.Spec.Containers, connector)
	return nil
}

type routingInfo struct {
	mode            string
	downstreamSteps []string
}

func (w *ConnectorWebhook) buildEnv(
	pod *corev1.Pod,
	engram *catalogv1alpha1.Engram,
	bindingValue string,
	bindingInfo *transportpb.BindingInfo,
	routing *routingInfo,
) []corev1.EnvVar {
	main := &pod.Spec.Containers[0]

	// Resolve hub endpoint based on routing mode
	hubEndpoint := resolveHubEndpointWithRouting(pod.Namespace, routing, pod)

	env := []corev1.EnvVar{
		{Name: contracts.ExecutionModeEnv, Value: "streaming"},
		{Name: contracts.TransportBindingEnv, Value: bindingValue},
		{Name: contracts.EngramNameEnv, Value: engram.Name},
		{Name: contracts.StoryNameEnv, Value: firstNonEmpty(
			engram.Annotations[storyAnnotationKey],
			getAnnotation(pod, storyAnnotationKey),
			pod.Labels[storyLabel],
		)},
		{Name: contracts.StepNameEnv, Value: firstNonEmpty(
			engram.Annotations[stepAnnotationKey],
			getAnnotation(pod, stepAnnotationKey),
			pod.Labels[stepLabel],
		)},
		{Name: contracts.StoryRunIDEnv, Value: firstNonEmpty(
			engram.Annotations[storyRunAnnotationKey],
			getAnnotation(pod, storyRunAnnotationKey),
			pod.Labels[contracts.StoryRunLabelKey],
			engram.Annotations[storyAnnotationKey],
		)},
		{Name: contracts.HubEndpointEnv, Value: hubEndpoint},
		{
			Name: contracts.PodNamespaceEnv,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"},
			},
		},
	}

	if value := lookupEnv(main.Env, contracts.GRPCPortEnv); value != "" {
		env = append(env, corev1.EnvVar{Name: contracts.GRPCPortEnv, Value: value})
	} else {
		env = append(env, corev1.EnvVar{Name: contracts.GRPCPortEnv, Value: defaultGRPCPort})
	}

	copyNames := []string{
		contracts.TransportSecurityModeEnv,
		contracts.ConnectorGenerationEnv,
		contracts.TracePropagationEnv,
		contracts.DebugEnv,
		contracts.GRPCMaxRecvBytesEnv,
		contracts.GRPCMaxSendBytesEnv,
		contracts.GRPCClientMaxRecvBytesEnv,
		contracts.GRPCClientMaxSendBytesEnv,
		contracts.GRPCDialTimeoutEnv,
		contracts.GRPCChannelBufferSizeEnv,
		contracts.GRPCReconnectMaxRetriesEnv,
		contracts.GRPCReconnectBaseBackoffEnv,
		contracts.GRPCReconnectMaxBackoffEnv,
		contracts.GRPCHangTimeoutEnv,
		contracts.GRPCMessageTimeoutEnv,
		contracts.GRPCChannelSendTimeoutEnv,
		contracts.GRPCHeartbeatIntervalEnv,
		contracts.TransportHeartbeatIntervalEnv,
		contracts.GRPCTLSCertFileEnv,
		contracts.GRPCTLSKeyFileEnv,
		contracts.GRPCCAFileEnv,
		contracts.GRPCClientCertFileEnv,
		contracts.GRPCClientKeyFileEnv,
		contracts.HubTLSCertFileEnv,
		contracts.HubTLSKeyFileEnv,
		contracts.GRPCKeepaliveTimeEnv,
		contracts.GRPCKeepaliveTimeoutEnv,
		contracts.PodNamespaceEnv,
	}
	for _, name := range copyNames {
		if val := lookupEnv(main.Env, name); val != "" {
			env = append(env, corev1.EnvVar{Name: name, Value: val})
		}
	}

	if lookupEnv(main.Env, contracts.GRPCClientCertFileEnv) == "" {
		if tlsCert := lookupEnv(main.Env, contracts.GRPCTLSCertFileEnv); tlsCert != "" {
			env = append(env, corev1.EnvVar{Name: contracts.GRPCClientCertFileEnv, Value: tlsCert})
		}
	}
	if lookupEnv(main.Env, contracts.GRPCClientKeyFileEnv) == "" {
		if tlsKey := lookupEnv(main.Env, contracts.GRPCTLSKeyFileEnv); tlsKey != "" {
			env = append(env, corev1.EnvVar{Name: contracts.GRPCClientKeyFileEnv, Value: tlsKey})
		}
	}
	if lookupEnv(main.Env, contracts.HubTLSCertFileEnv) == "" {
		if tlsCert := lookupEnv(main.Env, contracts.GRPCTLSCertFileEnv); tlsCert != "" {
			env = append(env, corev1.EnvVar{Name: contracts.HubTLSCertFileEnv, Value: tlsCert})
		}
	}
	if lookupEnv(main.Env, contracts.HubTLSKeyFileEnv) == "" {
		if tlsKey := lookupEnv(main.Env, contracts.GRPCTLSKeyFileEnv); tlsKey != "" {
			env = append(env, corev1.EnvVar{Name: contracts.HubTLSKeyFileEnv, Value: tlsKey})
		}
	}
	if lookupEnv(main.Env, contracts.HubCAFileEnv) == "" {
		if ca := lookupEnv(main.Env, contracts.GRPCCAFileEnv); ca != "" {
			env = append(env, corev1.EnvVar{Name: contracts.HubCAFileEnv, Value: ca})
		}
	}

	env = coretransport.AppendTransportMetadataEnv(env, bindingInfo)
	env = coretransport.AppendBindingEnvOverrides(env, bindingInfo)
	env = coretransport.SetOrReplaceEnvVar(env, corev1.EnvVar{Name: contracts.TransportSecurityModeEnv, Value: contracts.TransportSecurityModeTLS})
	return env
}

func (w *ConnectorWebhook) copyTLSVolumeMounts(pod *corev1.Pod) []corev1.VolumeMount {
	if len(pod.Spec.Containers) == 0 {
		return nil
	}
	var mounts []corev1.VolumeMount
	for _, mount := range pod.Spec.Containers[0].VolumeMounts {
		if strings.Contains(mount.Name, "engram-tls") {
			mounts = append(mounts, mount)
		}
	}
	return mounts
}

func getAnnotation(pod *corev1.Pod, key string) string {
	if pod == nil || pod.Annotations == nil {
		return ""
	}
	return pod.Annotations[key]
}

func lookupEnv(envs []corev1.EnvVar, name string) string {
	for _, env := range envs {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func resolveHubEndpoint(namespace string) string {
	if endpoint := strings.TrimSpace(os.Getenv(contracts.HubEndpointEnv)); endpoint != "" {
		if strings.Contains(endpoint, ":") {
			return endpoint
		}
		return fmt.Sprintf("%s:9000", endpoint)
	}

	serviceName := strings.TrimSpace(os.Getenv(contracts.HubServiceNameEnv))
	if serviceName == "" {
		serviceName = "bobravoz-grpc-hub"
	}

	serviceNamespace := strings.TrimSpace(os.Getenv(contracts.HubServiceNamespaceEnv))
	if serviceNamespace == "" {
		serviceNamespace = namespace
	}

	clusterDomain := strings.TrimSpace(os.Getenv(contracts.HubClusterDomainEnv))
	if clusterDomain == "" {
		clusterDomain = "svc.cluster.local"
	}

	host := serviceName
	if serviceNamespace != "" {
		host = fmt.Sprintf("%s.%s", host, serviceNamespace)
	}
	if clusterDomain != "" {
		host = fmt.Sprintf("%s.%s", host, clusterDomain)
	}

	port := strings.TrimSpace(os.Getenv(contracts.HubPortEnv))
	if port == "" {
		port = "9000"
	}

	return fmt.Sprintf("%s:%s", host, port)
}

func (w *ConnectorWebhook) resolveBindingEnvValue(ctx context.Context, defaultNamespace, annotation string) (string, error) {
	value := strings.TrimSpace(annotation)
	if value == "" {
		return "", nil
	}
	if strings.HasPrefix(value, "{") {
		return value, nil
	}
	namespace := defaultNamespace
	name := value
	if before, after, ok := strings.Cut(value, "/"); ok {
		if prefix := strings.TrimSpace(before); prefix != "" {
			if defaultNamespace != "" && prefix != defaultNamespace {
				return "", fmt.Errorf("cross-namespace transport binding references are not allowed: %s", value)
			}
			namespace = prefix
		}
		name = after
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil
	}
	var binding transportv1alpha1.TransportBinding
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if err := w.client.Get(ctx, key, &binding); err != nil {
		return "", fmt.Errorf("lookup transport binding for engram injection %s: %w", key.String(), err)
	}
	env, err := transportutil.EncodeBindingEnv(&binding)
	if err != nil {
		return "", fmt.Errorf("encode transport binding for engram injection %s: %w", key.String(), err)
	}
	return env, nil
}

func decodeBindingInfo(bindingValue string) (*transportpb.BindingInfo, error) {
	if strings.TrimSpace(bindingValue) == "" {
		return nil, nil
	}
	info, err := bindinginfo.Decode(bindingValue)
	if err != nil {
		return nil, fmt.Errorf("decode transport binding info: %w", err)
	}
	return info, nil
}

func (w *ConnectorWebhook) extractRoutingInfo(pod *corev1.Pod) *routingInfo {
	// Extract routing info from pod annotations (set by controller based on Story DAG analysis)
	// P2P routing is ENABLED BY DEFAULT and determined automatically based on:
	// - Single downstream step AND downstream doesn't need hub routing (no runtime config)
	// Can be overridden per-transport via Story transport settings: routing.mode = "hub"

	if pod.Annotations != nil {
		if mode := pod.Annotations["transport.bubustack.io/routing-mode"]; mode != "" {
			info := &routingInfo{mode: mode}
			if downstream := pod.Annotations["transport.bubustack.io/downstream-steps"]; downstream != "" {
				info.downstreamSteps = strings.Split(downstream, ",")
			}
			return info
		}
	}

	// Default to hub routing if annotations not present
	// (annotations should always be present from controller)
	return &routingInfo{mode: "hub"}
}

func resolveHubEndpointWithRouting(namespace string, routing *routingInfo, pod *corev1.Pod) string {
	// P2P routing is enabled by default when topology allows it
	// (single downstream without runtime config)
	if routing != nil && routing.mode == "p2p" && len(routing.downstreamSteps) == 1 {
		downstreamStep := routing.downstreamSteps[0]

		// Get StoryRun name from pod label (set by controller)
		// This is more reliable than parsing pod name which includes ReplicaSet hash
		storyRunName := pod.Labels[storyRunAnnotationKey]
		if storyRunName == "" {
			connectorWebhookLog.Error(nil, "P2P routing enabled but storyrun label not found, falling back to hub",
				"pod", pod.Name,
				"expectedLabel", storyRunAnnotationKey,
			)
			return resolveHubEndpoint(namespace)
		}

		// Build downstream service endpoint
		// Service name format: <storyrun>-<stepname>
		downstreamService := fmt.Sprintf("%s-%s", storyRunName, downstreamStep)
		downstreamEndpoint := fmt.Sprintf("%s.%s.svc.cluster.local:50051", downstreamService, namespace)

		connectorWebhookLog.Info("Using P2P routing",
			"from", pod.Name,
			"storyrun", storyRunName,
			"to", downstreamService,
			"endpoint", downstreamEndpoint,
		)
		return downstreamEndpoint
	}

	// Default to hub routing
	return resolveHubEndpoint(namespace)
}

func deriveConnectorSecurityContext(main *corev1.Container) *corev1.SecurityContext {
	if main != nil && main.SecurityContext != nil {
		clone := main.SecurityContext.DeepCopy()
		ensureDropCapabilities(clone)
		return clone
	}
	allow := false
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: &allow,
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
	}
}

func ensureDropCapabilities(sc *corev1.SecurityContext) {
	if sc.Capabilities == nil {
		sc.Capabilities = &corev1.Capabilities{}
	}
	if len(sc.Capabilities.Drop) == 0 {
		sc.Capabilities.Drop = []corev1.Capability{"ALL"}
	}
}
