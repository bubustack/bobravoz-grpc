package connector

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// bindingStatusReporter patches TransportBinding status fields based on connector observations
// and emits capability updates over the control plane.
type bindingStatusReporter struct {
	client            ctrlclient.Client
	key               types.NamespacedName
	log               logr.Logger
	info              *transportpb.BindingInfo
	heartbeatInterval time.Duration

	once      sync.Once
	startOnce sync.Once
	updates   chan capabilityObservation

	state   capabilityState
	stateMu sync.RWMutex

	listeners   map[chan capabilityState]struct{}
	listenersMu sync.Mutex

	heartbeatMu   sync.Mutex
	lastHeartbeat time.Time
}

type capabilityObservation struct {
	audio  *transportv1alpha1.AudioCodec
	video  *transportv1alpha1.VideoCodec
	binary string
}

type capabilityState struct {
	audio  *transportv1alpha1.AudioCodec
	video  *transportv1alpha1.VideoCodec
	binary string
}

func (s capabilityState) clone() capabilityState {
	return capabilityState{
		audio:  cloneAudioCodec(s.audio),
		video:  cloneVideoCodec(s.video),
		binary: s.binary,
	}
}

func (s capabilityState) isZero() bool {
	return s.audio == nil && s.video == nil && strings.TrimSpace(s.binary) == ""
}

func newBindingStatusReporter(cfg *Config, log logr.Logger) *bindingStatusReporter {
	if cfg.Binding.Name == "" || cfg.Binding.Namespace == "" {
		return nil
	}
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		log.Error(err, "transport connector status reporter disabled; unable to build in-cluster config")
		return nil
	}
	client, err := newReporterClient(restCfg)
	if err != nil {
		log.Error(err, "transport connector status reporter disabled; failed to init client")
		return nil
	}
	return &bindingStatusReporter{
		client:            client,
		key:               types.NamespacedName{Name: cfg.Binding.Name, Namespace: cfg.Binding.Namespace},
		log:               log.WithName("binding-reporter"),
		info:              cfg.Binding.Info,
		listeners:         make(map[chan capabilityState]struct{}),
		heartbeatInterval: cfg.BindingHeartbeatInterval,
	}
}

func newReporterClient(restCfg *rest.Config) (ctrlclient.Client, error) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	if err := transportv1alpha1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return ctrlclient.New(restCfg, ctrlclient.Options{Scheme: scheme})
}

// Start launches the asynchronous update loop. It is safe to call multiple times.
func (r *bindingStatusReporter) Start(ctx context.Context) {
	if r == nil {
		return
	}
	r.startOnce.Do(func() {
		r.updates = make(chan capabilityObservation, 32)
		go r.run(ctx)
		go r.heartbeatLoop(ctx)
	})
}

func (r *bindingStatusReporter) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case obs, ok := <-r.updates:
			if !ok {
				return
			}
			if err := r.applyObservation(ctx, obs); err != nil {
				r.log.Error(err, "failed to apply capability observation")
			}
		}
	}
}

// ReportReady performs the initial status patch and seeds the internal state.
func (r *bindingStatusReporter) ReportReady(ctx context.Context) {
	if r == nil {
		return
	}
	r.Start(ctx)
	r.once.Do(func() {
		if err := r.report(ctx); err != nil {
			r.log.Error(err, "failed to report binding readiness")
		}
	})
}

func (r *bindingStatusReporter) report(ctx context.Context) error {
	var binding transportv1alpha1.TransportBinding
	if err := r.client.Get(ctx, r.key, &binding); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	original := binding.DeepCopy()

	applyNegotiatedCapabilities(&binding, r.info)
	binding.Status.ObservedGeneration = binding.Generation
	cm := conditions.NewConditionManager(binding.Generation)
	cm.SetReadyCondition(&binding.Status.Conditions, true, conditions.ReasonTransportReady, "Connector reported negotiated codecs")

	if err := r.client.Status().Patch(ctx, &binding, ctrlclient.MergeFrom(original)); err != nil {
		return err
	}

	state := capabilityState{
		audio:  cloneAudioCodec(binding.Status.NegotiatedAudio),
		video:  cloneVideoCodec(binding.Status.NegotiatedVideo),
		binary: binding.Status.NegotiatedBinary,
	}
	r.setState(state)
	r.notifyListeners(state)
	return nil
}

func (r *bindingStatusReporter) applyObservation(ctx context.Context, obs capabilityObservation) error {
	if obs.isEmpty() {
		return nil
	}

	r.stateMu.Lock()
	changed := false
	if obs.audio != nil && !audioCodecEqual(r.state.audio, obs.audio) {
		r.state.audio = cloneAudioCodec(obs.audio)
		changed = true
	}
	if obs.video != nil && !videoCodecEqual(r.state.video, obs.video) {
		r.state.video = cloneVideoCodec(obs.video)
		changed = true
	}
	if obs.binary != "" && !strings.EqualFold(strings.TrimSpace(r.state.binary), strings.TrimSpace(obs.binary)) {
		r.state.binary = strings.TrimSpace(obs.binary)
		changed = true
	}
	state := r.state.clone()
	r.stateMu.Unlock()

	if !changed {
		return nil
	}

	if err := r.patchState(ctx, state); err != nil {
		return err
	}
	r.notifyListeners(state)
	return nil
}

func (r *bindingStatusReporter) patchState(ctx context.Context, state capabilityState) error {
	var binding transportv1alpha1.TransportBinding
	if err := r.client.Get(ctx, r.key, &binding); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	original := binding.DeepCopy()
	if state.audio != nil {
		codec := *state.audio
		binding.Status.NegotiatedAudio = &codec
	}
	if state.video != nil {
		codec := *state.video
		binding.Status.NegotiatedVideo = &codec
	}
	if strings.TrimSpace(state.binary) != "" {
		binding.Status.NegotiatedBinary = state.binary
	}
	binding.Status.ObservedGeneration = binding.Generation
	cm := conditions.NewConditionManager(binding.Generation)
	cm.SetReadyCondition(&binding.Status.Conditions, true, conditions.ReasonTransportReady, "Connector reported negotiated codecs")
	return r.client.Status().Patch(ctx, &binding, ctrlclient.MergeFrom(original))
}

func (r *bindingStatusReporter) setState(state capabilityState) {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	r.state = state.clone()
}

func (r *bindingStatusReporter) notifyListeners(state capabilityState) {
	if r == nil {
		return
	}
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()
	if len(r.listeners) == 0 {
		return
	}
	for ch := range r.listeners {
		select {
		case ch <- state.clone():
		default:
		}
	}
}

func (r *bindingStatusReporter) WatchCapabilities(ctx context.Context) <-chan capabilityState {
	ch := make(chan capabilityState, 1)
	if r == nil {
		close(ch)
		return ch
	}
	r.listenersMu.Lock()
	if r.listeners == nil {
		r.listeners = make(map[chan capabilityState]struct{})
	}
	r.listeners[ch] = struct{}{}
	current := r.state.clone()
	r.listenersMu.Unlock()

	if !current.isZero() {
		ch <- current
	}

	go func() {
		<-ctx.Done()
		r.removeListener(ch)
	}()
	return ch
}

func (r *bindingStatusReporter) removeListener(ch chan capabilityState) {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()
	if _, ok := r.listeners[ch]; ok {
		delete(r.listeners, ch)
		close(ch)
	}
}

func (r *bindingStatusReporter) enqueue(obs capabilityObservation) {
	if r == nil || obs.isEmpty() {
		return
	}
	if r.updates == nil {
		r.log.V(1).Info("dropping capability observation; reporter not started")
		return
	}
	select {
	case r.updates <- obs:
	default:
		r.log.V(1).Info("dropping capability observation; buffer full")
	}
}

func (r *bindingStatusReporter) ObserveAudioFrame(frame *transportpb.AudioFrame) {
	if frame == nil {
		return
	}
	codec := &transportv1alpha1.AudioCodec{
		Name:         normalizeCodecName(frame.GetCodec(), "pcm16"),
		SampleRateHz: frame.GetSampleRateHz(),
		Channels:     frame.GetChannels(),
	}
	r.enqueue(capabilityObservation{audio: codec})
}

func (r *bindingStatusReporter) ObserveVideoFrame(frame *transportpb.VideoFrame) {
	if frame == nil {
		return
	}
	profile := ""
	if frame.GetRaw() {
		profile = "raw"
	}
	codec := &transportv1alpha1.VideoCodec{
		Name:    normalizeCodecName(frame.GetCodec(), "h264"),
		Profile: profile,
	}
	r.enqueue(capabilityObservation{video: codec})
}

func (r *bindingStatusReporter) ObserveBinaryFrame(frame *transportpb.BinaryFrame) {
	if frame == nil {
		return
	}
	r.enqueue(capabilityObservation{binary: strings.TrimSpace(frame.GetMimeType())})
}

func (obs capabilityObservation) isEmpty() bool {
	return obs.audio == nil && obs.video == nil && strings.TrimSpace(obs.binary) == ""
}

// RecordHeartbeat updates the Ready condition timestamp so controllers can treat it
// as a connector heartbeat signal even when capabilities have not changed.
func (r *bindingStatusReporter) RecordHeartbeat(ctx context.Context, metadata map[string]string) {
	if r == nil {
		return
	}

	r.heartbeatMu.Lock()
	defer r.heartbeatMu.Unlock()

	var binding transportv1alpha1.TransportBinding
	if err := r.client.Get(ctx, r.key, &binding); err != nil {
		if !apierrors.IsNotFound(err) {
			r.log.Error(err, "failed to fetch TransportBinding for heartbeat")
		}
		return
	}

	original := binding.DeepCopy()
	message := heartbeatMessage(metadata)

	cm := conditions.NewConditionManager(binding.Generation)
	if cond := conditions.GetCondition(binding.Status.Conditions, conditions.ConditionReady); cond != nil {
		cond.Status = metav1.ConditionTrue
		cond.Reason = conditions.ReasonTransportReady
		cond.Message = message
		cond.LastTransitionTime = metav1.Now()
		cond.ObservedGeneration = binding.Generation
	} else {
		cm.SetReadyCondition(&binding.Status.Conditions, true, conditions.ReasonTransportReady, message)
	}
	if err := r.client.Status().Patch(ctx, &binding, ctrlclient.MergeFrom(original)); err != nil {
		r.log.Error(err, "failed to PATCH TransportBinding heartbeat")
		return
	}

	r.lastHeartbeat = time.Now()
}

func (r *bindingStatusReporter) heartbeatLoop(ctx context.Context) {
	if r == nil {
		return
	}
	interval := r.heartbeatInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.RecordHeartbeat(ctx, map[string]string{"source": "connector"})
		}
	}
}

// applyNegotiatedCapabilities updates the transport binding status using the available binding
// spec information and the inline BindingInfo payload exposed to the connector.
func applyNegotiatedCapabilities(binding *transportv1alpha1.TransportBinding, info *transportpb.BindingInfo) {
	if binding == nil {
		return
	}

	if binding.Status.NegotiatedAudio == nil {
		if codec := firstAudioCodec(binding.Spec.Audio); codec != nil {
			cp := *codec
			binding.Status.NegotiatedAudio = &cp
		} else if len(info.GetAudioCodecs()) > 0 {
			binding.Status.NegotiatedAudio = &transportv1alpha1.AudioCodec{Name: info.AudioCodecs[0]}
		}
	}

	if binding.Status.NegotiatedVideo == nil {
		if codec := firstVideoCodec(binding.Spec.Video); codec != nil {
			cp := *codec
			binding.Status.NegotiatedVideo = &cp
		} else if len(info.GetVideoCodecs()) > 0 {
			binding.Status.NegotiatedVideo = &transportv1alpha1.VideoCodec{Name: info.VideoCodecs[0]}
		}
	}

	if binding.Status.NegotiatedBinary == "" {
		if mime := firstBinaryMime(binding.Spec.Binary); mime != "" {
			binding.Status.NegotiatedBinary = mime
		} else if len(info.GetBinaryTypes()) > 0 {
			binding.Status.NegotiatedBinary = strings.TrimSpace(info.BinaryTypes[0])
		}
	}
}

func firstAudioCodec(binding *transportv1alpha1.AudioBinding) *transportv1alpha1.AudioCodec {
	if binding == nil || len(binding.Codecs) == 0 {
		return nil
	}
	return &binding.Codecs[0]
}

func firstVideoCodec(binding *transportv1alpha1.VideoBinding) *transportv1alpha1.VideoCodec {
	if binding == nil || len(binding.Codecs) == 0 {
		return nil
	}
	return &binding.Codecs[0]
}

func firstBinaryMime(binding *transportv1alpha1.BinaryBinding) string {
	if binding == nil || len(binding.MimeTypes) == 0 {
		return ""
	}
	return strings.TrimSpace(binding.MimeTypes[0])
}

// newFakeReporter is used in tests to bypass the in-cluster client wiring.
func newFakeReporter(client ctrlclient.Client, key types.NamespacedName, info *transportpb.BindingInfo, log logr.Logger) *bindingStatusReporter {
	return &bindingStatusReporter{
		client:    client,
		key:       key,
		log:       log,
		info:      info,
		listeners: make(map[chan capabilityState]struct{}),
	}
}

func cloneAudioCodec(codec *transportv1alpha1.AudioCodec) *transportv1alpha1.AudioCodec {
	if codec == nil {
		return nil
	}
	cp := *codec
	return &cp
}

func cloneVideoCodec(codec *transportv1alpha1.VideoCodec) *transportv1alpha1.VideoCodec {
	if codec == nil {
		return nil
	}
	cp := *codec
	return &cp
}

func audioCodecEqual(a, b *transportv1alpha1.AudioCodec) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(a.Name, b.Name) && a.SampleRateHz == b.SampleRateHz && a.Channels == b.Channels
}

func videoCodecEqual(a, b *transportv1alpha1.VideoCodec) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(a.Name, b.Name) && strings.EqualFold(strings.TrimSpace(a.Profile), strings.TrimSpace(b.Profile))
}

func normalizeCodecName(name, fallback string) string {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" {
		return fallback
	}
	return normalized
}

func heartbeatMessage(metadata map[string]string) string {
	if len(metadata) == 0 {
		return "connector heartbeat received"
	}
	values := make([]string, 0, len(metadata))
	for k, v := range metadata {
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(v)
		if key == "" || val == "" {
			continue
		}
		values = append(values, fmt.Sprintf("%s=%s", key, val))
	}
	if len(values) == 0 {
		return "connector heartbeat received"
	}
	sort.Strings(values)
	return fmt.Sprintf("connector heartbeat (%s)", strings.Join(values, ", "))
}
