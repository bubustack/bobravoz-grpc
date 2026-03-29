package config

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"
)

const manifestKindDeployment = "Deployment"

type configMapManifest struct {
	Kind string            `yaml:"kind"`
	Data map[string]string `yaml:"data"`
}

type chartValuesManifest struct {
	OperatorConfigData map[string]string `yaml:"operatorConfigData"`
}

type kustomizationManifest struct {
	Namespace  string `yaml:"namespace"`
	NamePrefix string `yaml:"namePrefix"`
}

type serviceManifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
}

type clusterRoleManifest struct {
	Kind  string `yaml:"kind"`
	Rules []struct {
		APIGroups []string `yaml:"apiGroups"`
		Resources []string `yaml:"resources"`
		Verbs     []string `yaml:"verbs"`
	} `yaml:"rules"`
}

type deploymentManifest struct {
	Kind string `yaml:"kind"`
	Spec struct {
		Template struct {
			Spec struct {
				Containers []struct {
					Name string `yaml:"name"`
					Env  []struct {
						Name  string `yaml:"name"`
						Value string `yaml:"value"`
					} `yaml:"env"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type webhookManifest struct {
	Kind     string `yaml:"kind"`
	Webhooks []struct {
		ClientConfig struct {
			Service struct {
				Name      string `yaml:"name"`
				Namespace string `yaml:"namespace"`
			} `yaml:"service"`
		} `yaml:"clientConfig"`
	} `yaml:"webhooks"`
}

type networkPolicyManifest struct {
	Kind string `yaml:"kind"`
	Spec struct {
		Ingress []struct {
			From []struct {
				NamespaceSelector map[string]any `yaml:"namespaceSelector"`
				PodSelector       struct {
					MatchLabels map[string]string `yaml:"matchLabels"`
				} `yaml:"podSelector"`
			} `yaml:"from"`
			Ports []struct {
				Port int32 `yaml:"port"`
			} `yaml:"ports"`
		} `yaml:"ingress"`
	} `yaml:"spec"`
}

func TestOperatorConfigManifestDefaultsToTLS(t *testing.T) {
	var manifest configMapManifest
	decodeSingleManifest(t, "config/manager/operator-config.yaml", &manifest)
	if got := manifest.Data["hub.transport-security-mode"]; got != "tls" {
		t.Fatalf("expected hub.transport-security-mode=tls, got %q", got)
	}
}

func TestOperatorConfigManifestUsesCanonicalConnectorImage(t *testing.T) {
	var manifest configMapManifest
	decodeSingleManifest(t, "config/manager/operator-config.yaml", &manifest)
	if got := manifest.Data["connector.image"]; got != DefaultConnectorImage {
		t.Fatalf("expected connector.image=%q, got %q", DefaultConnectorImage, got)
	}
}

func TestHelmChartDefaultsToTLS(t *testing.T) {
	var values chartValuesManifest
	decodeSingleManifest(t, "hack/charts/bobravoz-grpc/values.yaml", &values)
	if got := values.OperatorConfigData["hub.transport-security-mode"]; got != "tls" {
		t.Fatalf("expected Helm hub.transport-security-mode=tls, got %q", got)
	}
}

func TestManagerManifestUsesRenderedHubServiceName(t *testing.T) {
	var service serviceManifest
	decodeSingleManifest(t, "config/manager/service.yaml", &service)

	var defaults kustomizationManifest
	decodeSingleManifest(t, "config/default/kustomization.yaml", &defaults)

	var deployment deploymentManifest
	decodeMultiManifest(t, "config/manager/manager.yaml", func(doc []byte) {
		var candidate deploymentManifest
		if err := yaml.Unmarshal(doc, &candidate); err == nil && candidate.Kind == manifestKindDeployment {
			deployment = candidate
		}
	})

	if deployment.Kind != manifestKindDeployment {
		t.Fatal("deployment manifest not found in config/manager/manager.yaml")
	}
	if service.Metadata.Name == "" {
		t.Fatal("hub service manifest must declare a name")
	}
	if defaults.NamePrefix == "" {
		t.Fatal("default kustomization must declare namePrefix")
	}

	var got string
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != "manager" {
			continue
		}
		for _, env := range container.Env {
			if env.Name == "BUBU_HUB_SERVICE_NAME" {
				got = env.Value
				break
			}
		}
	}

	want := defaults.NamePrefix + service.Metadata.Name
	if got != want {
		t.Fatalf("expected BUBU_HUB_SERVICE_NAME=%q, got %q", want, got)
	}
}

func TestManagerManifestConnectorEnvMatchesCanonicalDefault(t *testing.T) {
	var deployment deploymentManifest
	decodeMultiManifest(t, "config/manager/manager.yaml", func(doc []byte) {
		var candidate deploymentManifest
		if err := yaml.Unmarshal(doc, &candidate); err == nil && candidate.Kind == manifestKindDeployment {
			deployment = candidate
		}
	})

	if deployment.Kind != manifestKindDeployment {
		t.Fatal("deployment manifest not found in config/manager/manager.yaml")
	}

	var got string
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != "manager" {
			continue
		}
		for _, env := range container.Env {
			if env.Name == "CONNECTOR_IMAGE" {
				got = env.Value
				break
			}
		}
	}

	if got != DefaultConnectorImage {
		t.Fatalf("expected CONNECTOR_IMAGE=%q, got %q", DefaultConnectorImage, got)
	}
}

func TestWebhookManifestMatchesWebhookService(t *testing.T) {
	var service serviceManifest
	decodeSingleManifest(t, "config/webhook/service.yaml", &service)

	var defaults kustomizationManifest
	decodeSingleManifest(t, "config/default/kustomization.yaml", &defaults)

	var webhook webhookManifest
	decodeMultiManifest(t, "config/webhook/manifests.yaml", func(doc []byte) {
		var candidate webhookManifest
		if err := yaml.Unmarshal(doc, &candidate); err == nil && candidate.Kind == "MutatingWebhookConfiguration" {
			webhook = candidate
		}
	})

	if webhook.Kind != "MutatingWebhookConfiguration" {
		t.Fatal("mutating webhook configuration not found")
	}
	if len(webhook.Webhooks) == 0 {
		t.Fatal("expected at least one webhook entry")
	}

	ref := webhook.Webhooks[0].ClientConfig.Service
	if ref.Name != service.Metadata.Name {
		t.Fatalf("expected webhook service name %q, got %q", service.Metadata.Name, ref.Name)
	}
	expectedNamespace := defaults.Namespace
	if expectedNamespace == "" {
		expectedNamespace = service.Metadata.Namespace
	}
	if ref.Namespace != expectedNamespace {
		t.Fatalf("expected webhook service namespace %q, got %q", expectedNamespace, ref.Namespace)
	}
}

func TestManagerRoleIncludesStepRunAccessForHandoffReporting(t *testing.T) {
	var role clusterRoleManifest
	decodeSingleManifest(t, "config/rbac/role.yaml", &role)
	if role.Kind != "ClusterRole" {
		t.Fatalf("expected ClusterRole manifest, got %q", role.Kind)
	}

	assertRoleRuleIncludesVerbs(t, role, "runs.bubustack.io", "stepruns", "get", "list", "watch")
	assertRoleRuleIncludesVerbs(t, role, "runs.bubustack.io", "stepruns/status", "get", "patch", "update")
}

func TestDefaultKustomizationEnablesNetworkPolicy(t *testing.T) {
	data := string(readRepoFile(t, "config/default/kustomization.yaml"))
	if !bytes.Contains([]byte(data), []byte("- ../network-policy")) {
		t.Fatal("expected config/default/kustomization.yaml to enable ../network-policy")
	}
}

func TestNetworkPolicyKustomizationIncludesHubAndProbePolicies(t *testing.T) {
	data := string(readRepoFile(t, "config/network-policy/kustomization.yaml"))
	for _, resource := range []string{
		"allow-hub-traffic.yaml",
		"allow-webhook-traffic.yaml",
		"allow-health-probe-traffic.yaml",
	} {
		if !bytes.Contains([]byte(data), []byte(resource)) {
			t.Fatalf("expected config/network-policy/kustomization.yaml to include %s", resource)
		}
	}
}

func TestHubNetworkPolicyRestrictsIngressToConnectorLabeledPods(t *testing.T) {
	var manifest networkPolicyManifest
	decodeSingleManifest(t, "config/network-policy/allow-hub-traffic.yaml", &manifest)
	if manifest.Kind != "NetworkPolicy" {
		t.Fatalf("expected NetworkPolicy manifest, got %q", manifest.Kind)
	}
	if len(manifest.Spec.Ingress) != 1 {
		t.Fatalf("expected one ingress rule, got %d", len(manifest.Spec.Ingress))
	}
	rule := manifest.Spec.Ingress[0]
	if len(rule.Ports) != 1 || rule.Ports[0].Port != 9000 {
		t.Fatalf("expected hub network policy to allow only port 9000, got %+v", rule.Ports)
	}
	if len(rule.From) != 1 {
		t.Fatalf("expected one ingress peer, got %d", len(rule.From))
	}
	if got := rule.From[0].PodSelector.MatchLabels["transport.bobravoz.bubustack.io/connector"]; got != "true" {
		t.Fatalf("expected connector label selector, got %q", got)
	}
	if rule.From[0].NamespaceSelector == nil {
		t.Fatal("expected hub network policy to allow matching connector pods from any namespace")
	}
}

func TestWebhookAndHealthNetworkPoliciesKeepRequiredPortsOpen(t *testing.T) {
	cases := []struct {
		path string
		port int32
	}{
		{path: "config/network-policy/allow-webhook-traffic.yaml", port: 9443},
		{path: "config/network-policy/allow-health-probe-traffic.yaml", port: 8081},
	}

	for _, tc := range cases {
		var manifest networkPolicyManifest
		decodeSingleManifest(t, tc.path, &manifest)
		if manifest.Kind != "NetworkPolicy" {
			t.Fatalf("expected %s to decode to NetworkPolicy, got %q", tc.path, manifest.Kind)
		}
		if len(manifest.Spec.Ingress) != 1 || len(manifest.Spec.Ingress[0].Ports) != 1 {
			t.Fatalf("expected %s to define exactly one open port", tc.path)
		}
		if got := manifest.Spec.Ingress[0].Ports[0].Port; got != tc.port {
			t.Fatalf("expected %s to allow port %d, got %d", tc.path, tc.port, got)
		}
	}
}

func decodeSingleManifest(t *testing.T, relPath string, out any) {
	t.Helper()
	data := readRepoFile(t, relPath)
	if err := yaml.Unmarshal(data, out); err != nil {
		t.Fatalf("unmarshal %s: %v", relPath, err)
	}
}

func decodeMultiManifest(t *testing.T, relPath string, fn func([]byte)) {
	t.Helper()
	data := readRepoFile(t, relPath)
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var doc any
		err := decoder.Decode(&doc)
		if err != nil {
			if err == io.EOF {
				return
			}
			t.Fatalf("decode %s: %v", relPath, err)
		}
		if doc == nil {
			continue
		}
		raw, err := yaml.Marshal(doc)
		if err != nil {
			t.Fatalf("re-marshal %s: %v", relPath, err)
		}
		fn(raw)
	}
}

func readRepoFile(t *testing.T, relPath string) []byte {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to resolve test file path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	data, err := os.ReadFile(filepath.Join(root, relPath))
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}
	return data
}

func assertRoleRuleIncludesVerbs(t *testing.T, role clusterRoleManifest, apiGroup, resource string, wantVerbs ...string) {
	t.Helper()

	for _, rule := range role.Rules {
		if !slices.Contains(rule.APIGroups, apiGroup) || !slices.Contains(rule.Resources, resource) {
			continue
		}
		for _, verb := range wantVerbs {
			if !slices.Contains(rule.Verbs, verb) {
				t.Fatalf("expected %s %s rule to include verb %q, got %v", apiGroup, resource, verb, rule.Verbs)
			}
		}
		return
	}

	t.Fatalf("expected %s %s rule in manager role", apiGroup, resource)
}
