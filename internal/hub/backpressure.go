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
	"encoding/json"
	"strings"
	"sync"
	"time"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	transportutil "github.com/bubustack/bobrapet/pkg/transport"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	transportDefaultsTTL    = 1 * time.Minute
	transportDefaultsMaxAge = 15 * time.Minute
)

type transportDefaultsEntry struct {
	settings   *runtime.RawExtension
	fetchedAt  time.Time
	lastAccess time.Time
}

// transportDefaultsCache is lazily populated during processing and guarded by
// its embedded mutex. Zero value is ready to use; entries are created on first
// Transport lookup and evicted on TTL expiry or fetch failure.
var transportDefaultsCache = struct {
	mu      sync.Mutex
	entries map[string]transportDefaultsEntry
}{}

type transportSettings struct {
	Backpressure  *backpressureConfig  `json:"backpressure,omitempty"`
	FlowControl   *flowControlConfig   `json:"flowControl,omitempty"`
	Delivery      *deliveryConfig      `json:"delivery,omitempty"`
	Routing       *routingConfig       `json:"routing,omitempty"`
	Lanes         []laneConfig         `json:"lanes,omitempty"`
	FanIn         *fanInConfig         `json:"fanIn,omitempty"`
	Partitioning  *partitioningConfig  `json:"partitioning,omitempty"`
	Lifecycle     *lifecycleConfig     `json:"lifecycle,omitempty"`
	Observability *observabilityConfig `json:"observability,omitempty"`
	Recording     *recordingConfig     `json:"recording,omitempty"`
}

type backpressureConfig struct {
	Buffer *bufferSettings `json:"buffer,omitempty"`
}

type bufferSettings struct {
	MaxMessages   *int   `json:"maxMessages,omitempty"`
	MaxBytes      *int   `json:"maxBytes,omitempty"`
	MaxAgeSeconds *int   `json:"maxAgeSeconds,omitempty"`
	DropPolicy    string `json:"dropPolicy,omitempty"`
}

type flowControlConfig struct {
	Mode            string           `json:"mode,omitempty"`
	InitialCredits  *flowCredits     `json:"initialCredits,omitempty"`
	AckEvery        *flowAckSettings `json:"ackEvery,omitempty"`
	PauseThreshold  *flowThreshold   `json:"pauseThreshold,omitempty"`
	ResumeThreshold *flowThreshold   `json:"resumeThreshold,omitempty"`
}

type flowCredits struct {
	Messages *int `json:"messages,omitempty"`
	Bytes    *int `json:"bytes,omitempty"`
}

type flowAckSettings struct {
	Messages *int    `json:"messages,omitempty"`
	Bytes    *int    `json:"bytes,omitempty"`
	MaxDelay *string `json:"maxDelay,omitempty"`
}

type flowThreshold struct {
	BufferPct *int32 `json:"bufferPct,omitempty"`
}

type deliveryConfig struct {
	Ordering  string        `json:"ordering,omitempty"`
	Semantics string        `json:"semantics,omitempty"`
	Replay    *replayConfig `json:"replay,omitempty"`
}

type replayConfig struct {
	Mode               string  `json:"mode,omitempty"`
	RetentionSeconds   *int    `json:"retentionSeconds,omitempty"`
	CheckpointInterval *string `json:"checkpointInterval,omitempty"`
}

type routingConfig struct {
	Mode           string              `json:"mode,omitempty"`
	FanOut         string              `json:"fanOut,omitempty"`
	MaxDownstreams *int32              `json:"maxDownstreams,omitempty"`
	Rules          []routingRuleConfig `json:"rules,omitempty"`
}

type routingRuleConfig struct {
	Name   string               `json:"name,omitempty"`
	When   *string              `json:"when,omitempty"`
	Action string               `json:"action,omitempty"`
	Target *routingTargetConfig `json:"target,omitempty"`
}

type routingTargetConfig struct {
	Steps []string `json:"steps,omitempty"`
}

type laneConfig struct {
	Name        string `json:"name,omitempty"`
	Kind        string `json:"kind,omitempty"`
	Direction   string `json:"direction,omitempty"`
	Description string `json:"description,omitempty"`
	MaxMessages *int32 `json:"maxMessages,omitempty"`
	MaxBytes    *int32 `json:"maxBytes,omitempty"`
}

type fanInConfig struct {
	Mode           string `json:"mode,omitempty"`
	Quorum         *int32 `json:"quorum,omitempty"`
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty"`
	MaxEntries     *int32 `json:"maxEntries,omitempty"`
}

type partitioningConfig struct {
	Mode       string  `json:"mode,omitempty"`
	Key        *string `json:"key,omitempty"`
	Partitions *int32  `json:"partitions,omitempty"`
	Sticky     *bool   `json:"sticky,omitempty"`
}

type lifecycleConfig struct {
	Strategy            string `json:"strategy,omitempty"`
	DrainTimeoutSeconds *int32 `json:"drainTimeoutSeconds,omitempty"`
	MaxInFlight         *int32 `json:"maxInFlight,omitempty"`
}

type observabilityConfig struct {
	Metrics   *toggleConfig    `json:"metrics,omitempty"`
	Tracing   *tracingConfig   `json:"tracing,omitempty"`
	Watermark *watermarkConfig `json:"watermark,omitempty"`
}

type toggleConfig struct {
	Enabled *bool `json:"enabled,omitempty"`
}

type tracingConfig struct {
	Enabled      *bool   `json:"enabled,omitempty"`
	SampleRate   *int32  `json:"sampleRate,omitempty"`
	SamplePolicy *string `json:"samplePolicy,omitempty"`
}

type watermarkConfig struct {
	Enabled         *bool   `json:"enabled,omitempty"`
	TimestampSource *string `json:"timestampSource,omitempty"`
}

type recordingConfig struct {
	Mode             string   `json:"mode,omitempty"`
	SampleRate       *int32   `json:"sampleRate,omitempty"`
	RetentionSeconds *int32   `json:"retentionSeconds,omitempty"`
	RedactFields     []string `json:"redactFields,omitempty"`
}

func resolveBufferLimitsForStep(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) bufferLimits {
	if step == nil {
		return defaultBufferLimits()
	}
	return resolveBufferLimits(ctx, reader, story, step.Transport)
}

func resolveBufferLimits(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, transportName string) bufferLimits {
	limits := defaultBufferLimits()
	parsed := resolveTransportSettings(ctx, reader, story, transportName)
	if parsed == nil || parsed.Backpressure == nil || parsed.Backpressure.Buffer == nil {
		if parsed != nil && len(parsed.Lanes) > 0 {
			limits = applyLaneLimits(limits, parsed.Lanes)
		}
		return limits
	}

	buf := parsed.Backpressure.Buffer
	if buf.MaxMessages != nil {
		limits.maxMessages = *buf.MaxMessages
	}
	if buf.MaxBytes != nil {
		limits.maxBytes = *buf.MaxBytes
	}
	if buf.MaxAgeSeconds != nil && *buf.MaxAgeSeconds > 0 {
		limits.maxAge = time.Duration(*buf.MaxAgeSeconds) * time.Second
	}
	if policy := strings.TrimSpace(buf.DropPolicy); policy != "" {
		limits.dropPolicy = bufferDropPolicy(strings.ToLower(policy))
	}

	if len(parsed.Lanes) > 0 {
		limits = applyLaneLimits(limits, parsed.Lanes)
	}
	return normalizeBufferLimits(limits)
}

func applyLaneLimits(limits bufferLimits, lanes []laneConfig) bufferLimits {
	if len(lanes) == 0 {
		return limits
	}
	if limits.laneMaxMessages == nil {
		limits.laneMaxMessages = make(map[string]int)
	}
	if limits.laneMaxBytes == nil {
		limits.laneMaxBytes = make(map[string]int)
	}
	for _, lane := range lanes {
		kind := strings.ToLower(strings.TrimSpace(lane.Kind))
		if kind == "" {
			continue
		}
		if lane.MaxMessages != nil && *lane.MaxMessages > 0 {
			limits.laneMaxMessages[kind] = int(*lane.MaxMessages)
		}
		if lane.MaxBytes != nil && *lane.MaxBytes > 0 {
			limits.laneMaxBytes[kind] = int(*lane.MaxBytes)
		}
	}
	return limits
}

// transportSettingsCacheKey is the context key for the per-packet settings cache.
type transportSettingsCacheKey struct{}

// transportSettingsCache is a request-scoped cache that deduplicates
// resolveTransportSettings calls for the same transport within a single
// packet dispatch. Each resolve*ForStep call for the same step.Transport
// hits the same cached value instead of repeating the K8s API lookup and
// JSON unmarshal.
type transportSettingsCache struct {
	mu    sync.Mutex
	cache map[string]*transportSettings
}

func (c *transportSettingsCache) get(
	ctx context.Context,
	reader client.Reader,
	story *bubuv1alpha1.Story,
	transportName string,
) *transportSettings {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cache == nil {
		c.cache = make(map[string]*transportSettings)
	}
	if v, ok := c.cache[transportName]; ok {
		return v
	}
	// Call the uncached resolver directly to avoid re-entering the cache check.
	v := resolveTransportSettingsUncached(ctx, reader, story, transportName)
	c.cache[transportName] = v
	return v
}

// withTransportSettingsCache returns a context carrying a fresh, empty
// transportSettingsCache. Use this at the start of per-packet dispatch
// functions so that all resolve*ForStep calls within the same dispatch
// share a single cache, avoiding redundant K8s lookups.
func withTransportSettingsCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, transportSettingsCacheKey{}, &transportSettingsCache{})
}

// resolveTransportSettingsUncached performs the raw K8s lookup and JSON unmarshal
// with no caching. Prefer resolveTransportSettings which adds a per-packet cache layer.
func resolveTransportSettingsUncached(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, transportName string) *transportSettings {
	settings := resolveStoryTransportSettings(ctx, reader, story, transportName)
	if settings == nil || len(settings.Raw) == 0 {
		return nil
	}
	var parsed transportSettings
	if err := json.Unmarshal(settings.Raw, &parsed); err != nil {
		log.Log.WithName("backpressure").V(1).Info(
			"Failed to parse transport settings",
			"transport", transportName,
			"err", err,
		)
		return nil
	}
	return &parsed
}

func resolveTransportSettings(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, transportName string) *transportSettings {
	// Serve from the request-scoped cache when available to avoid redundant
	// K8s API lookups and JSON unmarshals for the same transport per packet.
	if c, ok := ctx.Value(transportSettingsCacheKey{}).(*transportSettingsCache); ok {
		return c.get(ctx, reader, story, transportName)
	}
	return resolveTransportSettingsUncached(ctx, reader, story, transportName)
}

//nolint:gocyclo // Transport policy parsing is clearer when the nested config cases stay together.
func resolveFlowControlPolicyForStep(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, limits bufferLimits) flowControlPolicy {
	if step == nil {
		return flowControlPolicy{mode: flowControlNone}
	}
	parsed := resolveTransportSettings(ctx, reader, story, step.Transport)
	if parsed == nil || parsed.FlowControl == nil {
		return flowControlPolicy{mode: flowControlNone}
	}
	cfg := parsed.FlowControl
	mode := flowControlMode(strings.ToLower(strings.TrimSpace(cfg.Mode)))
	switch mode {
	case flowControlCredits, flowControlWindow:
	default:
		if mode != "" {
			log.Log.WithName("flow-control").V(1).Info("Unknown flow control mode; defaulting to none", "mode", cfg.Mode)
		}
		return flowControlPolicy{mode: flowControlNone}
	}

	limits = normalizeBufferLimits(limits)
	policy := flowControlPolicy{
		mode:                mode,
		initialCreditsMsg:   limits.maxMessages,
		initialCreditsBytes: limits.maxBytes,
		ackEveryMessages:    1,
	}
	if cfg.InitialCredits != nil {
		if cfg.InitialCredits.Messages != nil && *cfg.InitialCredits.Messages > 0 {
			policy.initialCreditsMsg = *cfg.InitialCredits.Messages
		}
		if cfg.InitialCredits.Bytes != nil && *cfg.InitialCredits.Bytes > 0 {
			policy.initialCreditsBytes = *cfg.InitialCredits.Bytes
		}
	}
	if cfg.AckEvery != nil {
		if cfg.AckEvery.Messages != nil && *cfg.AckEvery.Messages > 0 {
			policy.ackEveryMessages = *cfg.AckEvery.Messages
		}
		if cfg.AckEvery.Bytes != nil && *cfg.AckEvery.Bytes > 0 {
			policy.ackEveryBytes = *cfg.AckEvery.Bytes
		}
		if cfg.AckEvery.MaxDelay != nil && strings.TrimSpace(*cfg.AckEvery.MaxDelay) != "" {
			if d, err := time.ParseDuration(strings.TrimSpace(*cfg.AckEvery.MaxDelay)); err == nil && d > 0 {
				policy.ackEveryDelay = d
			} else {
				log.Log.WithName("flow-control").V(1).Info("Invalid ackEvery.maxDelay; ignoring", "value", *cfg.AckEvery.MaxDelay)
			}
		}
	}
	if cfg.PauseThreshold != nil && cfg.PauseThreshold.BufferPct != nil {
		policy.pauseThresholdPct = clampPct(*cfg.PauseThreshold.BufferPct)
	}
	if cfg.ResumeThreshold != nil && cfg.ResumeThreshold.BufferPct != nil {
		policy.resumeThresholdPct = clampPct(*cfg.ResumeThreshold.BufferPct)
	}
	if policy.pauseThresholdPct > 0 && policy.resumeThresholdPct <= 0 {
		policy.resumeThresholdPct = policy.pauseThresholdPct * 0.5
	}
	if policy.resumeThresholdPct > policy.pauseThresholdPct && policy.pauseThresholdPct > 0 {
		policy.resumeThresholdPct = policy.pauseThresholdPct * 0.5
	}
	return policy
}

//nolint:gocyclo // Delivery parsing keeps ordering, semantics, and replay interactions in one place.
func resolveDeliveryPolicyForStep(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) deliveryPolicy {
	policy := deliveryPolicy{
		ordering:  orderingNone,
		semantics: semanticsBestEffort,
		replay: replayPolicy{
			mode: replayNone,
		},
	}
	if step == nil {
		return policy
	}
	parsed := resolveTransportSettings(ctx, reader, story, step.Transport)
	if parsed == nil || parsed.Delivery == nil {
		return policy
	}
	cfg := parsed.Delivery
	if cfg.Ordering != "" {
		switch strings.ToLower(strings.TrimSpace(cfg.Ordering)) {
		case "per_stream":
			policy.ordering = orderingPerStream
		case "per_partition":
			policy.ordering = orderingPerPartition
		case "none":
			policy.ordering = orderingNone
		default:
			log.Log.WithName("delivery").V(1).Info("Unknown ordering mode; defaulting to none", "ordering", cfg.Ordering)
		}
	}
	if cfg.Semantics != "" {
		switch strings.ToLower(strings.TrimSpace(cfg.Semantics)) {
		case "at_least_once":
			policy.semantics = semanticsAtLeastOnce
		case "best_effort":
			policy.semantics = semanticsBestEffort
		default:
			log.Log.WithName("delivery").V(1).Info("Unknown semantics; defaulting to best_effort", "semantics", cfg.Semantics)
		}
	}
	if cfg.Replay != nil {
		switch strings.ToLower(strings.TrimSpace(cfg.Replay.Mode)) {
		case string(replayMemory):
			policy.replay.mode = replayMemory
		case string(replayDurable):
			policy.replay.mode = replayDurable
		case string(replayNone), "":
			policy.replay.mode = replayNone
		default:
			log.Log.WithName("delivery").V(1).Info("Unknown replay mode; defaulting to none", "mode", cfg.Replay.Mode)
		}
		if cfg.Replay.RetentionSeconds != nil && *cfg.Replay.RetentionSeconds > 0 {
			policy.replay.retention = time.Duration(*cfg.Replay.RetentionSeconds) * time.Second
		}
		if cfg.Replay.CheckpointInterval != nil && strings.TrimSpace(*cfg.Replay.CheckpointInterval) != "" {
			if d, err := time.ParseDuration(strings.TrimSpace(*cfg.Replay.CheckpointInterval)); err == nil && d > 0 {
				policy.replay.checkpointInterval = d
			} else {
				log.Log.WithName("delivery").V(1).Info("Invalid replay.checkpointInterval; ignoring", "value", *cfg.Replay.CheckpointInterval)
			}
		}
	}
	if policy.replay.mode != replayNone && policy.semantics == semanticsBestEffort {
		policy.semantics = semanticsAtLeastOnce
	}
	if policy.semantics == semanticsAtLeastOnce && policy.ordering == orderingNone {
		policy.ordering = orderingPerStream
	}
	if policy.replay.mode != replayNone && policy.replay.checkpointInterval <= 0 {
		policy.replay.checkpointInterval = time.Second
	}
	return policy
}

type routingPolicy struct {
	mode           string
	fanOut         string
	maxDownstreams int
	rules          []routingRule
	hasAllowRules  bool
}

type routingRuleAction string

const (
	routingRuleAllow routingRuleAction = "allow"
	routingRuleDeny  routingRuleAction = "deny"
)

type routingTarget struct {
	all   bool
	steps map[string]struct{}
}

func (t routingTarget) matches(stepID string) bool {
	if t.all {
		return true
	}
	if stepID == "" {
		return false
	}
	_, ok := t.steps[stepID]
	return ok
}

type routingRule struct {
	name   string
	when   *string
	action routingRuleAction
	target routingTarget
}

//nolint:gocyclo // Routing policy evaluation is easier to follow as one parser over the transport config.
func resolveRoutingPolicyForStep(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) routingPolicy {
	policy := routingPolicy{mode: "auto", fanOut: "sequential"}
	if step == nil {
		return policy
	}
	parsed := resolveTransportSettings(ctx, reader, story, step.Transport)
	if parsed == nil || parsed.Routing == nil {
		return policy
	}
	mode := strings.ToLower(strings.TrimSpace(parsed.Routing.Mode))
	switch mode {
	case "auto", "hub", "p2p":
		policy.mode = mode
	case "":
	default:
		log.Log.WithName("routing").V(1).Info("Unknown routing mode; defaulting to auto", "mode", parsed.Routing.Mode)
	}
	fanOut := strings.ToLower(strings.TrimSpace(parsed.Routing.FanOut))
	switch fanOut {
	case "sequential", "parallel":
		policy.fanOut = fanOut
	case "":
	default:
		log.Log.WithName("routing").V(1).Info("Unknown fanOut mode; defaulting to sequential", "fanOut", parsed.Routing.FanOut)
	}
	if parsed.Routing.MaxDownstreams != nil && *parsed.Routing.MaxDownstreams > 0 {
		policy.maxDownstreams = int(*parsed.Routing.MaxDownstreams)
	}
	if len(parsed.Routing.Rules) > 0 {
		for _, rule := range parsed.Routing.Rules {
			action := strings.ToLower(strings.TrimSpace(rule.Action))
			if action == "" {
				action = string(routingRuleAllow)
			}
			switch action {
			case string(routingRuleAllow), string(routingRuleDeny):
			default:
				log.Log.WithName("routing").V(1).Info("Unknown routing rule action; skipping", "action", rule.Action, "rule", rule.Name)
				continue
			}
			target := routingTarget{all: true}
			if rule.Target != nil && len(rule.Target.Steps) > 0 {
				target.all = false
				target.steps = make(map[string]struct{}, len(rule.Target.Steps))
				for _, stepName := range rule.Target.Steps {
					trimmed := strings.TrimSpace(stepName)
					if trimmed == "" {
						continue
					}
					target.steps[trimmed] = struct{}{}
				}
				if len(target.steps) == 0 {
					target.all = true
				}
			}
			policy.rules = append(policy.rules, routingRule{
				name:   strings.TrimSpace(rule.Name),
				when:   rule.When,
				action: routingRuleAction(action),
				target: target,
			})
			if action == string(routingRuleAllow) {
				policy.hasAllowRules = true
			}
		}
	}
	return policy
}

type fanInPolicy struct {
	mode       string
	quorum     int
	timeout    time.Duration
	hasTimeout bool
	maxEntries int
}

func resolveFanInPolicyForStep(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) fanInPolicy {
	policy := fanInPolicy{mode: "all"}
	if step == nil {
		return policy
	}
	parsed := resolveTransportSettings(ctx, reader, story, step.Transport)
	if parsed == nil || parsed.FanIn == nil {
		return policy
	}
	cfg := parsed.FanIn
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	switch mode {
	case "all", "any", "quorum":
		policy.mode = mode
	case "":
	default:
		log.Log.WithName("fanin").V(1).Info("Unknown fan-in mode; defaulting to all", "mode", cfg.Mode)
	}
	if cfg.Quorum != nil && *cfg.Quorum > 0 {
		policy.quorum = int(*cfg.Quorum)
	}
	if cfg.TimeoutSeconds != nil {
		policy.hasTimeout = true
		if *cfg.TimeoutSeconds > 0 {
			policy.timeout = time.Duration(*cfg.TimeoutSeconds) * time.Second
		}
	}
	if cfg.MaxEntries != nil && *cfg.MaxEntries > 0 {
		policy.maxEntries = int(*cfg.MaxEntries)
	}
	return policy
}

type partitioningPolicy struct {
	mode       string
	key        string
	partitions int
	sticky     bool
}

func resolvePartitioningPolicyForStep(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) partitioningPolicy {
	policy := partitioningPolicy{mode: "none", partitions: 0}
	if step == nil {
		return policy
	}
	parsed := resolveTransportSettings(ctx, reader, story, step.Transport)
	if parsed == nil || parsed.Partitioning == nil {
		return policy
	}
	mode := strings.ToLower(strings.TrimSpace(parsed.Partitioning.Mode))
	switch mode {
	case "none", "preserve", "hash":
		policy.mode = mode
	case "":
	default:
		log.Log.WithName("partitioning").V(1).Info("Unknown partitioning mode; defaulting to none", "mode", parsed.Partitioning.Mode)
	}
	if parsed.Partitioning.Key != nil {
		policy.key = strings.TrimSpace(*parsed.Partitioning.Key)
	}
	if parsed.Partitioning.Partitions != nil && *parsed.Partitioning.Partitions > 0 {
		policy.partitions = int(*parsed.Partitioning.Partitions)
	}
	if parsed.Partitioning.Sticky != nil {
		policy.sticky = *parsed.Partitioning.Sticky
	}
	return policy
}

type lifecyclePolicy struct {
	maxInFlight  int
	drainTimeout time.Duration
	strategy     string
	drainEnabled bool
}

func resolveLifecyclePolicyForStep(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) lifecyclePolicy {
	policy := lifecyclePolicy{}
	if step == nil {
		return policy
	}
	parsed := resolveTransportSettings(ctx, reader, story, step.Transport)
	if parsed == nil || parsed.Lifecycle == nil {
		return policy
	}
	if parsed.Lifecycle.MaxInFlight != nil && *parsed.Lifecycle.MaxInFlight > 0 {
		policy.maxInFlight = int(*parsed.Lifecycle.MaxInFlight)
	}
	if parsed.Lifecycle.DrainTimeoutSeconds != nil && *parsed.Lifecycle.DrainTimeoutSeconds > 0 {
		policy.drainTimeout = time.Duration(*parsed.Lifecycle.DrainTimeoutSeconds) * time.Second
	}
	if parsed.Lifecycle.Strategy != "" {
		mode := strings.ToLower(strings.TrimSpace(parsed.Lifecycle.Strategy))
		switch mode {
		case "rolling", "drain_cutover", "blue_green":
			policy.strategy = mode
		default:
			log.Log.WithName("lifecycle").V(1).Info("Unknown lifecycle strategy; ignoring", "strategy", parsed.Lifecycle.Strategy)
		}
	}
	if policy.drainTimeout > 0 {
		if policy.strategy == "" {
			policy.drainEnabled = true
		} else {
			policy.drainEnabled = policy.strategy == "drain_cutover" || policy.strategy == "blue_green"
		}
	}
	return policy
}

type observabilityPolicy struct {
	metricsEnabled    bool
	tracingEnabled    bool
	traceSampleRate   int
	traceSamplePolicy string
	watermarkEnabled  bool
	watermarkSource   string
}

//nolint:gocyclo // Observability parsing keeps related knobs and defaults localized.
func resolveObservabilityPolicyForStep(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) observabilityPolicy {
	policy := observabilityPolicy{
		metricsEnabled: true,
		tracingEnabled: true,
	}
	explicitTraceSample := false
	if step == nil {
		return policy
	}
	parsed := resolveTransportSettings(ctx, reader, story, step.Transport)
	if parsed == nil || parsed.Observability == nil {
		return policy
	}
	if parsed.Observability.Metrics != nil && parsed.Observability.Metrics.Enabled != nil {
		policy.metricsEnabled = *parsed.Observability.Metrics.Enabled
	}
	if parsed.Observability.Tracing != nil {
		if parsed.Observability.Tracing.Enabled != nil {
			policy.tracingEnabled = *parsed.Observability.Tracing.Enabled
		}
		if parsed.Observability.Tracing.SampleRate != nil {
			policy.traceSampleRate = clampPercent(int(*parsed.Observability.Tracing.SampleRate))
			explicitTraceSample = true
		}
		if parsed.Observability.Tracing.SamplePolicy != nil {
			policy.traceSamplePolicy = strings.TrimSpace(*parsed.Observability.Tracing.SamplePolicy)
		}
	}
	if parsed.Observability.Watermark != nil {
		if parsed.Observability.Watermark.Enabled != nil {
			policy.watermarkEnabled = *parsed.Observability.Watermark.Enabled
		}
		if parsed.Observability.Watermark.TimestampSource != nil {
			policy.watermarkSource = strings.TrimSpace(*parsed.Observability.Watermark.TimestampSource)
		}
	}
	if policy.tracingEnabled && policy.traceSampleRate == 0 && !explicitTraceSample {
		policy.traceSampleRate = 100
	}
	return policy
}

type recordingPolicy struct {
	mode         string
	sampleRate   int
	retention    time.Duration
	redactFields []string
}

func resolveRecordingPolicyForStep(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) recordingPolicy {
	policy := recordingPolicy{mode: recordingModeOff, sampleRate: 0}
	if step == nil {
		return policy
	}
	parsed := resolveTransportSettings(ctx, reader, story, step.Transport)
	if parsed == nil || parsed.Recording == nil {
		return policy
	}
	mode := strings.ToLower(strings.TrimSpace(parsed.Recording.Mode))
	switch mode {
	case recordingModeOff, recordingModeMetadata, recordingModePayload:
		policy.mode = mode
	case "":
	default:
		log.Log.WithName("recording").V(1).Info("Unknown recording mode; defaulting to off", "mode", parsed.Recording.Mode)
	}
	if parsed.Recording.SampleRate != nil {
		policy.sampleRate = clampPercent(int(*parsed.Recording.SampleRate))
	}
	if parsed.Recording.RetentionSeconds != nil && *parsed.Recording.RetentionSeconds > 0 {
		policy.retention = time.Duration(*parsed.Recording.RetentionSeconds) * time.Second
	}
	if len(parsed.Recording.RedactFields) > 0 {
		policy.redactFields = append([]string(nil), parsed.Recording.RedactFields...)
	}
	if policy.mode != recordingModeOff && policy.sampleRate == 0 {
		policy.sampleRate = 100
	}
	return policy
}

func clampPercent(val int) int {
	if val <= 0 {
		return 0
	}
	if val >= 100 {
		return 100
	}
	return val
}

func clampPct(v int32) float64 {
	if v <= 0 {
		return 0
	}
	if v >= 100 {
		return 1
	}
	return float64(v) / 100.0
}

func resolveStoryTransportSettings(ctx context.Context, reader client.Reader, story *bubuv1alpha1.Story, transportName string) *runtime.RawExtension {
	decl := resolveStoryTransportDecl(story, transportName)
	if decl == nil {
		return nil
	}
	settings, err := transportutil.MergeSettingsWithStreaming(decl.Settings, decl.Streaming)
	if err != nil {
		log.Log.WithName("backpressure").V(1).Info(
			"Failed to encode story transport streaming settings",
			"transport", transportName,
			"err", err,
		)
		settings = decl.Settings
	}
	if reader == nil {
		return settings
	}
	ref := strings.TrimSpace(decl.TransportRef)
	if ref == "" {
		return settings
	}
	defaults := getTransportDefaultSettings(ctx, reader, ref, transportName)
	if defaults == nil {
		return settings
	}
	mergedRaw, err := transportutil.MergeSettings(defaults, settings)
	if err != nil {
		log.Log.WithName("backpressure").V(1).Info(
			"Failed to merge transport settings",
			"transportRef", ref,
			"transport", transportName,
			"err", err,
		)
		return settings
	}
	if len(mergedRaw) == 0 {
		return settings
	}
	return &runtime.RawExtension{Raw: mergedRaw}
}

func getTransportDefaultSettings(ctx context.Context, reader client.Reader, ref string, transportName string) *runtime.RawExtension {
	if reader == nil || strings.TrimSpace(ref) == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now()
	transportDefaultsCache.mu.Lock()
	entry, ok := transportDefaultsCache.entries[ref]
	if ok && now.Sub(entry.fetchedAt) < transportDefaultsTTL {
		entry.lastAccess = now
		transportDefaultsCache.entries[ref] = entry
		transportDefaultsCache.mu.Unlock()
		return cloneRawExtension(entry.settings)
	}
	// Periodically evict entries that haven't been accessed in maxAge.
	evictStaleTransportDefaults(now)
	transportDefaultsCache.mu.Unlock()

	// Use a short timeout to avoid blocking processPacket when the Transport
	// Informer hasn't synced yet.  The caller uses the gRPC stream context
	// which has no deadline, so reader.Get can block indefinitely waiting for
	// the Informer cache to sync.  A 2-second timeout ensures we fall back to
	// default settings quickly and keep the message loop running.
	getCtx, getCancel := context.WithTimeout(ctx, 2*time.Second)
	defer getCancel()
	var transport transportv1alpha1.Transport
	if err := reader.Get(getCtx, types.NamespacedName{Name: ref}, &transport); err != nil {
		log.Log.WithName("backpressure").Error(err, "Failed to resolve transport defaults", "transportRef", ref, "transport", transportName)
		// Evict stale cache entry on any error (e.g. Transport CR deleted).
		// Without eviction, a deleted Transport would serve stale defaults
		// until the TTL expires. With eviction, the next packet will attempt
		// a fresh lookup and fall back to defaults on repeated failure.
		transportDefaultsCache.mu.Lock()
		delete(transportDefaultsCache.entries, ref)
		transportDefaultsCache.mu.Unlock()
		return nil
	}
	settings, err := transportutil.MergeSettingsWithStreaming(transport.Spec.DefaultSettings, transport.Spec.Streaming)
	if err != nil {
		log.Log.WithName("backpressure").V(1).Info("Failed to encode transport streaming defaults", "transportRef", ref, "transport", transportName, "err", err)
		settings = cloneRawExtension(transport.Spec.DefaultSettings)
	}
	transportDefaultsCache.mu.Lock()
	if transportDefaultsCache.entries == nil {
		transportDefaultsCache.entries = make(map[string]transportDefaultsEntry)
	}
	transportDefaultsCache.entries[ref] = transportDefaultsEntry{settings: cloneRawExtension(settings), fetchedAt: now, lastAccess: now}
	transportDefaultsCache.mu.Unlock()
	return settings
}

// evictStaleTransportDefaults removes cache entries that haven't been accessed
// within transportDefaultsMaxAge. Must be called with transportDefaultsCache.mu held.
func evictStaleTransportDefaults(now time.Time) {
	if len(transportDefaultsCache.entries) == 0 {
		return
	}
	for ref, entry := range transportDefaultsCache.entries {
		if now.Sub(entry.lastAccess) > transportDefaultsMaxAge {
			delete(transportDefaultsCache.entries, ref)
		}
	}
}

func cloneRawExtension(src *runtime.RawExtension) *runtime.RawExtension {
	if src == nil || len(src.Raw) == 0 {
		return nil
	}
	return &runtime.RawExtension{Raw: append([]byte(nil), src.Raw...)}
}

func resolveStoryTransportDecl(story *bubuv1alpha1.Story, transportName string) *bubuv1alpha1.StoryTransport {
	if story == nil {
		return nil
	}
	name := strings.TrimSpace(transportName)
	if name == "" {
		if len(story.Spec.Transports) == 0 {
			return nil
		}
		return &story.Spec.Transports[0]
	}
	for i := range story.Spec.Transports {
		if story.Spec.Transports[i].Name == name {
			return &story.Spec.Transports[i]
		}
	}
	return nil
}
