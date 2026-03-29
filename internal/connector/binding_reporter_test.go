package connector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
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

	ctx := t.Context()

	const iters = 200
	done := make(chan struct{})
	// Writer: repeatedly set r.state via stateMu.
	go func() {
		defer close(done)
		for range iters {
			r.stateMu.Lock()
			r.state = capabilityState{}
			r.stateMu.Unlock()
		}
	}()

	// Reader: repeatedly call WatchCapabilities (clones r.state).
	for range iters {
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
	ctx := t.Context()
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
	ctx := t.Context()
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

	ctx := t.Context()
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

func TestBindingStatusReporter_ReportReadyRetriesWhenBindingAppearsLater(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	key := types.NamespacedName{Name: testBindingName, Namespace: testBindingNamespace}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&transportv1alpha1.TransportBinding{}).Build()
	reporter := newFakeReporter(client, key, &transportpb.BindingInfo{}, logr.Discard())

	ctx := t.Context()
	reporter.Start(ctx)
	reporter.ReportReady(ctx)

	binding := &transportv1alpha1.TransportBinding{}
	binding.Name = key.Name
	binding.Namespace = key.Namespace
	require.NoError(t, client.Create(ctx, binding))

	reporter.ReportReady(ctx)

	require.Eventually(t, func() bool {
		var updated transportv1alpha1.TransportBinding
		if err := client.Get(context.Background(), key, &updated); err != nil {
			return false
		}
		return conditions.GetCondition(updated.Status.Conditions, conditions.ConditionReady) != nil
	}, time.Second, 10*time.Millisecond)
}

func TestBindingStatusReporter_PatchStateRetriesConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	binding := &transportv1alpha1.TransportBinding{}
	binding.Name = testBindingName
	binding.Namespace = testBindingNamespace

	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(binding).WithObjects(binding).Build()
	client := &conflictOnceClient{
		Client: baseClient,
		writer: &conflictOnceStatusWriter{SubResourceWriter: baseClient.Status()},
	}

	reporter := newFakeReporter(client, types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &transportpb.BindingInfo{}, logr.Discard())
	err := reporter.patchState(context.Background(), capabilityState{
		audio: &transportv1alpha1.AudioCodec{Name: "opus", SampleRateHz: 48000, Channels: 2},
	})
	require.NoError(t, err)

	var updated transportv1alpha1.TransportBinding
	require.NoError(t, baseClient.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &updated))
	require.NotNil(t, updated.Status.NegotiatedAudio)
	require.Equal(t, "opus", updated.Status.NegotiatedAudio.Name)
}

func TestBindingStatusReporter_RecordHeartbeatSanitizesMetadata(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	binding := &transportv1alpha1.TransportBinding{}
	binding.Name = testBindingName
	binding.Namespace = testBindingNamespace

	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(binding).WithObjects(binding).Build()
	reporter := newFakeReporter(client, types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &transportpb.BindingInfo{}, logr.Discard())
	ctx := t.Context()
	reporter.Start(ctx)

	reporter.RecordHeartbeat(ctx, map[string]string{
		"source": "connector",
		"status": "alive",
		"token":  "secret",
	})

	require.Eventually(t, func() bool {
		var updated transportv1alpha1.TransportBinding
		if err := client.Get(context.Background(), types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &updated); err != nil {
			return false
		}
		cond := conditions.GetCondition(updated.Status.Conditions, conditions.ConditionReady)
		if cond == nil {
			return false
		}
		return cond.Message == "connector heartbeat (source=connector, status=alive)"
	}, time.Second, 10*time.Millisecond)
}

type conflictOnceClient struct {
	ctrlclient.Client
	writer ctrlclient.SubResourceWriter
}

func (c *conflictOnceClient) Status() ctrlclient.SubResourceWriter {
	return c.writer
}

type conflictOnceStatusWriter struct {
	ctrlclient.SubResourceWriter
	conflicted bool
}

func (w *conflictOnceStatusWriter) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.SubResourcePatchOption) error {
	if !w.conflicted {
		w.conflicted = true
		return apierrors.NewConflict(
			schema.GroupResource{Group: "transport.bobrapet.bubustack.io", Resource: "transportbindings"},
			obj.GetName(),
			errors.New("conflict"),
		)
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

// TestBindingReporterShutdownNoChannelPanic verifies that concurrent shutdown
// and enqueue operations do not cause a send on closed channel panic.
func TestBindingReporterShutdownNoChannelPanic(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	binding := &transportv1alpha1.TransportBinding{}
	binding.Name = testBindingName
	binding.Namespace = testBindingNamespace

	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(binding).WithObjects(binding).Build()
	reporter := newFakeReporter(client, types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &transportpb.BindingInfo{}, logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())
	reporter.Start(ctx)

	// Allow the run goroutine to start.
	time.Sleep(10 * time.Millisecond)

	// Run multiple goroutines that enqueue observations while shutdown occurs.
	var wg sync.WaitGroup
	const numWriters = 10
	const messagesPerWriter = 50

	for i := range numWriters {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range messagesPerWriter {
				reporter.enqueue(capabilityObservation{
					audio: &transportv1alpha1.AudioCodec{
						Name:         "opus",
						SampleRateHz: int32(48000 + id*100 + j),
						Channels:     2,
					},
				})
				// Small random delay to increase chance of race.
				if j%5 == 0 {
					time.Sleep(time.Microsecond)
				}
			}
		}(i)
	}

	// Trigger shutdown partway through.
	go func() {
		time.Sleep(time.Millisecond)
		cancel()
	}()

	// This should not panic.
	require.NotPanics(t, func() {
		wg.Wait()
	})
}

// TestBindingReporterDoubleClose verifies that calling closeUpdates multiple
// times does not panic.
func TestBindingReporterDoubleClose(t *testing.T) {
	reporter := &bindingStatusReporter{
		log:       logr.Discard(),
		listeners: make(map[chan capabilityState]struct{}),
		updates:   make(chan capabilityObservation, 32),
	}

	require.NotPanics(t, func() {
		reporter.closeUpdates()
		reporter.closeUpdates()
		reporter.closeUpdates()
	})

	// Verify the channel is actually closed.
	select {
	case _, ok := <-reporter.updates:
		if ok {
			t.Fatal("expected channel to be closed")
		}
	default:
		t.Fatal("expected channel read to return immediately on closed channel")
	}
}

// TestBindingReporterEnqueueAfterClose verifies that enqueue after close does
// not panic but gracefully drops the observation.
func TestBindingReporterEnqueueAfterClose(t *testing.T) {
	reporter := &bindingStatusReporter{
		log:       logr.Discard(),
		key:       types.NamespacedName{Name: "test", Namespace: "default"},
		listeners: make(map[chan capabilityState]struct{}),
		updates:   make(chan capabilityObservation, 32),
	}

	reporter.closeUpdates()

	require.NotPanics(t, func() {
		reporter.enqueue(capabilityObservation{
			audio: &transportv1alpha1.AudioCodec{Name: "opus"},
		})
	})
}
