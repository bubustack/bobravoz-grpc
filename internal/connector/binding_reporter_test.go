package connector

import (
	"context"
	"testing"
	"time"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testBindingName      = "binding-1"
	testBindingNamespace = "default"
)

// TestWatchCapabilities_NoRaceWithApplyObservation verifies that concurrent
// WatchCapabilities and applyObservation calls do not race on r.state.
// Run with -race to catch the previously missing stateMu lock.
func TestWatchCapabilities_NoRaceWithApplyObservation(t *testing.T) {
	r := &bindingStatusReporter{
		log:       logr.Discard(),
		listeners: make(map[chan capabilityState]struct{}),
		updates:   make(chan capabilityObservation, 32),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const iters = 200
	done := make(chan struct{})
	// Writer: repeatedly set r.state via stateMu.
	go func() {
		defer close(done)
		for i := 0; i < iters; i++ {
			r.stateMu.Lock()
			r.state = capabilityState{}
			r.stateMu.Unlock()
		}
	}()

	// Reader: repeatedly call WatchCapabilities (clones r.state).
	for i := 0; i < iters; i++ {
		ch := r.WatchCapabilities(ctx)
		// drain the channel to avoid blocking the sender goroutine
		select {
		case <-ch:
		default:
		}
	}
	<-done
}

func TestBindingStatusReporter_ReportReady(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	binding := &transportv1alpha1.TransportBinding{}
	binding.Name = testBindingName
	binding.Namespace = testBindingNamespace
	binding.Spec.Audio = &transportv1alpha1.AudioBinding{
		Codecs: []transportv1alpha1.AudioCodec{
			{Name: "pcm16", SampleRateHz: 16000, Channels: 1},
		},
	}
	binding.Spec.Binary = &transportv1alpha1.BinaryBinding{
		MimeTypes: []string{"application/json"},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(binding).WithObjects(binding).Build()
	reporter := newFakeReporter(client, types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &transportpb.BindingInfo{
		VideoCodecs: []string{"h264"},
	}, logr.Discard())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter.Start(ctx)

	require.NotPanics(t, func() {
		reporter.ReportReady(ctx)
	})

	var updated transportv1alpha1.TransportBinding
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &updated))
	require.NotNil(t, updated.Status.NegotiatedAudio)
	require.Equal(t, "pcm16", updated.Status.NegotiatedAudio.Name)
	require.NotNil(t, updated.Status.NegotiatedVideo)
	require.Equal(t, "h264", updated.Status.NegotiatedVideo.Name)
	require.Equal(t, "application/json", updated.Status.NegotiatedBinary)
}

func TestBindingStatusReporter_ObserveAudioFrameUpdatesStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	binding := &transportv1alpha1.TransportBinding{}
	binding.Name = testBindingName
	binding.Namespace = testBindingNamespace
	binding.Spec.Audio = &transportv1alpha1.AudioBinding{
		Codecs: []transportv1alpha1.AudioCodec{{Name: "pcm16", SampleRateHz: 16000, Channels: 1}},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(binding).WithObjects(binding).Build()
	reporter := newFakeReporter(client, types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &transportpb.BindingInfo{}, logr.Discard())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter.Start(ctx)
	reporter.ReportReady(ctx)
	updates := reporter.WatchCapabilities(ctx)
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("expected initial capability state")
	}

	reporter.ObserveAudioFrame(&transportpb.AudioFrame{Codec: "opus", SampleRateHz: 48000, Channels: 2})

	require.Eventually(t, func() bool {
		var updated transportv1alpha1.TransportBinding
		if err := client.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &updated); err != nil {
			return false
		}
		return updated.Status.NegotiatedAudio != nil && updated.Status.NegotiatedAudio.Name == "opus"
	}, time.Second, 10*time.Millisecond)

	select {
	case state := <-updates:
		require.NotNil(t, state.audio)
		require.Equal(t, "opus", state.audio.Name)
	case <-time.After(time.Second):
		t.Fatal("expected capability update from watcher")
	}
}

func TestBindingStatusReporter_RecordHeartbeatUpdatesCondition(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	binding := &transportv1alpha1.TransportBinding{}
	binding.Name = testBindingName
	binding.Namespace = testBindingNamespace

	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(binding).WithObjects(binding).Build()
	reporter := newFakeReporter(client, types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &transportpb.BindingInfo{}, logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reporter.Start(ctx)
	reporter.ReportReady(ctx)

	require.Eventually(t, func() bool {
		var updated transportv1alpha1.TransportBinding
		if err := client.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &updated); err != nil {
			return false
		}
		return conditions.GetCondition(updated.Status.Conditions, conditions.ConditionReady) != nil
	}, time.Second, 10*time.Millisecond)

	reporter.RecordHeartbeat(ctx, map[string]string{"status": "alive"})

	time.Sleep(50 * time.Millisecond)

	require.Eventually(t, func() bool {
		var updated transportv1alpha1.TransportBinding
		if err := client.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &updated); err != nil {
			return false
		}
		cond := conditions.GetCondition(updated.Status.Conditions, conditions.ConditionReady)
		if cond == nil {
			return false
		}
		return cond.Message == "connector heartbeat (status=alive)"
	}, time.Second, 10*time.Millisecond)
}
