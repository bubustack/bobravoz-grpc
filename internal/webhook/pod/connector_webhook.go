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

	if pod.Annotations != nil && pod.Annotations[connectorInjectedAnnotation] == "true" {
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

	bindingValue := ""
	if pod.Annotations != nil {
		bindingValue = strings.TrimSpace(pod.Annotations[contracts.TransportBindingAnnotation])
	}
	if bindingValue == "" {
		ref := engram.Annotations[contracts.TransportBindingAnnotation]
		resolved, err := w.resolveBindingEnvValue(ctx, pod.Namespace, ref)
		if err != nil {
			return admission.Errored(http.StatusBadRequest, err)
		}
		bindingValue = resolved
	}
	if bindingValue == "" {
		return admission.Allowed("engram not bound to transport")
	}

	mutated := pod.DeepCopy()
	if mutated.Annotations == nil {
		mutated.Annotations = make(map[string]string)
	}

	w.injectConnector(mutated, engram, bindingValue)
	mutated.Annotations[contracts.TransportBindingAnnotation] = bindingValue
	mutated.Annotations[connectorInjectedAnnotation] = "true"
	marshaled, err := json.Marshal(mutated)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}

// SetupWithManager registers the webhook with the controller-runtime server.
func (w *ConnectorWebhook) SetupWithManager(mgr ctrl.Manager) error {
	mgr.GetWebhookServer().Register("/mutate-transport-connector-pod", &admission.Webhook{Handler: w})
	return nil
}

func (w *ConnectorWebhook) injectConnector(pod *corev1.Pod, engram *catalogv1alpha1.Engram, bindingValue string) {
	if len(pod.Spec.Containers) == 0 {
		return
	}

	main := &pod.Spec.Containers[0]
	bindingInfo := decodeBindingInfo(bindingValue)

	env := w.buildEnv(pod, engram, bindingValue, bindingInfo)
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
}

func (w *ConnectorWebhook) buildEnv(
	pod *corev1.Pod,
	engram *catalogv1alpha1.Engram,
	bindingValue string,
	bindingInfo *transportpb.BindingInfo,
) []corev1.EnvVar {
	main := &pod.Spec.Containers[0]
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
		{Name: contracts.HubEndpointEnv, Value: resolveHubEndpoint(pod.Namespace)},
		{
			Name: contracts.PodNamespaceEnv,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
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
		contracts.GRPCAFileEnv,
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
		if ca := lookupEnv(main.Env, contracts.GRPCAFileEnv); ca != "" {
			env = append(env, corev1.EnvVar{Name: contracts.HubCAFileEnv, Value: ca})
		}
	}

	env = coretransport.AppendTransportMetadataEnv(env, bindingInfo)
	env = coretransport.AppendBindingEnvOverrides(env, bindingInfo)
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
	if slash := strings.Index(value, "/"); slash >= 0 {
		if prefix := strings.TrimSpace(value[:slash]); prefix != "" {
			namespace = prefix
		}
		name = value[slash+1:]
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil
	}
	var binding transportv1alpha1.TransportBinding
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if err := w.client.Get(ctx, key, &binding); err != nil {
		return "", fmt.Errorf("fetch transport binding %s: %w", key.String(), err)
	}
	env, err := transportutil.EncodeBindingEnv(&binding)
	if err != nil {
		return "", fmt.Errorf("encode transport binding %s: %w", key.String(), err)
	}
	return env, nil
}

func decodeBindingInfo(bindingValue string) *transportpb.BindingInfo {
	if strings.TrimSpace(bindingValue) == "" {
		return nil
	}
	info, err := bindinginfo.Decode(bindingValue)
	if err != nil {
		connectorWebhookLog.Error(err, "failed to decode transport binding info", "binding", bindingValue)
		return nil
	}
	return info
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
