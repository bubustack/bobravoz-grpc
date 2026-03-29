package hub

import (
	"context"
	"testing"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestTemplateSchemaCache_RefreshesOnTemplateUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))

	initialSchema := mustMarshalJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"value": map[string]any{"type": "string"},
		},
	})
	updatedSchema := mustMarshalJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"value": map[string]any{"type": "integer"},
		},
	})
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl", ResourceVersion: "1"},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			InputSchema: &runtime.RawExtension{Raw: initialSchema},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	cache := newTemplateSchemaCache(k8sClient)

	_, _, raw, err := cache.Get(context.Background(), "tmpl")
	require.NoError(t, err)
	assert.JSONEq(t, string(initialSchema), string(raw))

	var updated catalogv1alpha1.EngramTemplate
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Name: "tmpl"}, &updated))
	updated.Spec.InputSchema = &runtime.RawExtension{Raw: updatedSchema}
	require.NoError(t, k8sClient.Update(context.Background(), &updated))

	_, _, raw, err = cache.Get(context.Background(), "tmpl")
	require.NoError(t, err)
	assert.JSONEq(t, string(updatedSchema), string(raw))
}

func TestTemplateSchemaCache_DoesNotServeDeletedTemplate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))

	schema := mustMarshalJSON(t, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"value": map[string]any{"type": "string"},
		},
	})
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl", ResourceVersion: "1"},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			InputSchema: &runtime.RawExtension{Raw: schema},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	cache := newTemplateSchemaCache(k8sClient)

	_, _, _, err := cache.Get(context.Background(), "tmpl")
	require.NoError(t, err)
	require.NoError(t, k8sClient.Delete(context.Background(), template))

	_, _, _, err = cache.Get(context.Background(), "tmpl")
	require.Error(t, err)
}
