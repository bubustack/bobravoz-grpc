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
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"math"
	"math/big"
	mathrand "math/rand/v2"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bobravoz-grpc/pkg/metrics"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	metadataEnvelopeTimeKey  = "bubu.envelope.timestamp_ms"
	recordingTimestampFormat = "20060102T150405.000000000Z"
)

var (
	recordingRetentionPolicy = storage.RetentionPolicyFromEnv()
	recordingGCLock          sync.Mutex
	recordingGCLastRun       = make(map[string]time.Time)
)

func shouldSamplePercent(rate int) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 100 {
		return true
	}
	return mathrand.IntN(100) < rate
}

func shouldSampleTrace(policy observabilityPolicy) bool {
	if !policy.tracingEnabled {
		return false
	}
	policyName := strings.ToLower(strings.TrimSpace(policy.traceSamplePolicy))
	switch policyName {
	case "always":
		return true
	case "never":
		return false
	case "", "rate", "random":
		return shouldSamplePercent(policy.traceSampleRate)
	default:
		return shouldSamplePercent(policy.traceSampleRate)
	}
}

func maybeStartPacketSpan(ctx context.Context, policy observabilityPolicy, storyRunName, stepID string, packet *transportpb.DataPacket) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !shouldSampleTrace(policy) {
		return ctx, func() {}
	}
	attrs := []attribute.KeyValue{
		attribute.String("storyrun", storyRunName),
		attribute.String("step", stepID),
	}
	if packet != nil {
		attrs = append(attrs,
			attribute.Bool("has_payload", packet.GetPayload() != nil),
			attribute.Bool("has_inputs", packet.GetInputs() != nil),
			attribute.Bool("has_audio", packet.GetAudio() != nil),
			attribute.Bool("has_video", packet.GetVideo() != nil),
			attribute.Bool("has_binary", packet.GetBinary() != nil),
			attribute.Int("packet_bytes", proto.Size(packet)),
		)
	}
	tracer := otel.Tracer("bobravoz-hub")
	ctx, span := tracer.Start(ctx, "hub.processPacket", trace.WithAttributes(attrs...))
	return ctx, func() { span.End() }
}

func applyPartitioningPolicy(packet *transportpb.DataPacket, policy partitioningPolicy) {
	if packet == nil {
		return
	}
	env := packet.GetEnvelope()
	if env == nil {
		env = &transportpb.StreamEnvelope{}
		packet.Envelope = env
	}
	if policy.sticky && env.Partition != "" {
		return
	}
	mode := strings.ToLower(strings.TrimSpace(policy.mode))
	switch mode {
	case string(flowControlNone), "":
		return
	case "preserve":
		if env.Partition != "" {
			return
		}
		value := resolvePartitionKeyValue(packet, policy)
		if value == "" {
			return
		}
		env.Partition = sanitizePartitionValue(value)
		return
	case "hash":
		value := resolvePartitionKeyValue(packet, policy)
		if value == "" {
			return
		}
		env.Partition = hashPartitionValue(value, policy.partitions)
		return
	default:
		return
	}
}

func resolvePartitionKeyValue(packet *transportpb.DataPacket, policy partitioningPolicy) string {
	if packet == nil {
		return ""
	}
	key := strings.TrimSpace(policy.key)
	if key == "" {
		return lookupDefaultPartitionKey(packet.Metadata)
	}
	parts := strings.Split(key, ".")
	if len(parts) == 0 {
		return ""
	}
	prefix := strings.ToLower(strings.TrimSpace(parts[0]))
	rest := parts[1:]
	switch prefix {
	case recordingModeMetadata:
		return valueFromMetadata(packet.Metadata, rest)
	case recordingModePayload:
		return valueFromStruct(packet.GetPayload(), rest)
	case "inputs":
		return valueFromStruct(packet.GetInputs(), rest)
	case "envelope":
		return valueFromEnvelope(packet.GetEnvelope(), rest)
	default:
		return valueFromMetadata(packet.Metadata, parts)
	}
}

func lookupDefaultPartitionKey(metadata map[string]string) string {
	if metadata == nil {
		return ""
	}
	candidates := []string{
		"partition_key",
		"partition-key",
		"bubu.partition_key",
		"bubu.partition",
	}
	for _, key := range candidates {
		if value := strings.TrimSpace(metadata[key]); value != "" {
			return value
		}
	}
	return ""
}

func valueFromMetadata(metadata map[string]string, path []string) string {
	if metadata == nil {
		return ""
	}
	if len(path) == 0 {
		return ""
	}
	value, ok := metadata[path[0]]
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func valueFromStruct(st *structpb.Struct, path []string) string {
	if st == nil {
		return ""
	}
	if len(path) == 0 {
		return ""
	}
	root := st.AsMap()
	val, ok := valueFromMap(root, path)
	if !ok {
		return ""
	}
	return stringifyValue(val)
}

func valueFromEnvelope(env *transportpb.StreamEnvelope, path []string) string {
	if env == nil || len(path) == 0 {
		return ""
	}
	switch strings.ToLower(path[0]) {
	case "stream_id", "streamid":
		return env.GetStreamId()
	case "sequence":
		if env.GetSequence() == 0 {
			return ""
		}
		return strconv.FormatUint(env.GetSequence(), 10)
	case "partition":
		return env.GetPartition()
	default:
		return ""
	}
}

func valueFromMap(root map[string]any, path []string) (any, bool) {
	if len(path) == 0 {
		return nil, false
	}
	current := root
	for i := range path {
		segment := strings.TrimSpace(path[i])
		if segment == "" {
			return nil, false
		}
		value, ok := current[segment]
		if !ok {
			return nil, false
		}
		if i == len(path)-1 {
			return value, true
		}
		next, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		current = next
	}
	return nil, false
}

func stringifyValue(val any) string {
	if val == nil {
		return ""
	}
	switch typed := val.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	case float64:
		if math.Mod(typed, 1) == 0 {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		if math.Mod(float64(typed), 1) == 0 {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}

func sanitizePartitionValue(val string) string {
	clean := strings.TrimSpace(val)
	if clean == "" {
		return ""
	}
	if len(clean) <= 128 {
		return clean
	}
	return hashPartitionValue(clean, 0)
}

func hashPartitionValue(val string, partitions int) string {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(val))
	sum := hasher.Sum32()
	if partitions > 0 {
		return fmt.Sprintf("p%d", int(sum)%partitions)
	}
	return fmt.Sprintf("h%x", sum)
}

func extractEventTime(packet *transportpb.DataPacket, source string) (time.Time, bool) {
	if packet == nil {
		return time.Time{}, false
	}
	source = strings.TrimSpace(source)
	if source != "" {
		if ts, ok := extractTimestampFromSource(packet, source); ok {
			return ts, true
		}
		return time.Time{}, false
	}
	if ts, ok := timestampFromMetadata(packet.Metadata, metadataEnvelopeTimeKey); ok {
		return ts, true
	}
	if audio := packet.GetAudio(); audio != nil && audio.TimestampMs > 0 {
		return time.UnixMilli(int64(audio.TimestampMs)), true
	}
	if video := packet.GetVideo(); video != nil && video.TimestampMs > 0 {
		return time.UnixMilli(int64(video.TimestampMs)), true
	}
	if binary := packet.GetBinary(); binary != nil && binary.TimestampMs > 0 {
		return time.UnixMilli(int64(binary.TimestampMs)), true
	}
	return time.Time{}, false
}

func extractTimestampFromSource(packet *transportpb.DataPacket, source string) (time.Time, bool) {
	parts := strings.Split(source, ".")
	if len(parts) == 0 {
		return time.Time{}, false
	}
	prefix := strings.ToLower(strings.TrimSpace(parts[0]))
	rest := parts[1:]
	switch prefix {
	case recordingModeMetadata:
		if len(rest) == 0 {
			return time.Time{}, false
		}
		return timestampFromMetadata(packet.Metadata, strings.Join(rest, "."))
	case "audio":
		if packet.GetAudio() == nil || packet.GetAudio().TimestampMs == 0 {
			return time.Time{}, false
		}
		return time.UnixMilli(int64(packet.GetAudio().TimestampMs)), true
	case "video":
		if packet.GetVideo() == nil || packet.GetVideo().TimestampMs == 0 {
			return time.Time{}, false
		}
		return time.UnixMilli(int64(packet.GetVideo().TimestampMs)), true
	case "binary":
		if packet.GetBinary() == nil || packet.GetBinary().TimestampMs == 0 {
			return time.Time{}, false
		}
		return time.UnixMilli(int64(packet.GetBinary().TimestampMs)), true
	default:
		return timestampFromMetadata(packet.Metadata, source)
	}
}

func timestampFromMetadata(metadata map[string]string, key string) (time.Time, bool) {
	if metadata == nil {
		return time.Time{}, false
	}
	raw, ok := metadata[key]
	if !ok {
		return time.Time{}, false
	}
	return parseTimestampValue(raw)
}

func parseTimestampValue(raw string) (time.Time, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, false
	}
	if val, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return unixFromNumeric(val), true
	}
	if val, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return unixFromNumeric(int64(val)), true
	}
	if t, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return t, true
	}
	return time.Time{}, false
}

func unixFromNumeric(val int64) time.Time {
	if val > 1_000_000_000_000 {
		return time.UnixMilli(val)
	}
	if val > 1_000_000_000 {
		return time.Unix(val, 0)
	}
	return time.UnixMilli(val)
}

func maybeRecordStreamPacket(ctx context.Context, store *storage.StorageManager, storyRunName, storyRunNS, stepID string, packet *transportpb.DataPacket, policy recordingPolicy) {
	if packet == nil || policy.mode == recordingModeOff {
		return
	}
	if store == nil {
		metrics.RecordHubRecording(storyRunName, stepID, policy.mode, "storage_disabled")
		return
	}
	if !shouldSamplePercent(policy.sampleRate) {
		metrics.RecordHubRecording(storyRunName, stepID, policy.mode, "sampled_out")
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	data, err := buildRecordingPayload(packet, storyRunName, storyRunNS, stepID, policy)
	if err != nil {
		metrics.RecordHubRecording(storyRunName, stepID, policy.mode, "encode_error")
		return
	}
	path := recordingStoragePath(storyRunNS, storyRunName, stepID)
	if err := store.WriteBlob(ctx, path, "application/json", data); err != nil {
		metrics.RecordHubRecording(storyRunName, stepID, policy.mode, "write_error")
		return
	}
	metrics.RecordHubRecording(storyRunName, stepID, policy.mode, "recorded")
	maybeEnforceRecordingRetention(ctx, store, storyRunName, storyRunNS, stepID, policy)
}

func buildRecordingPayload(packet *transportpb.DataPacket, storyRunName, storyRunNS, stepID string, policy recordingPolicy) ([]byte, error) {
	record := map[string]any{
		"recorded_at":         time.Now().UTC().Format(time.RFC3339Nano),
		"story_run":           storyRunName,
		"story_run_namespace": storyRunNS,
		"step":                stepID,
		"mode":                policy.mode,
	}
	if policy.retention > 0 {
		record["retention_seconds"] = int64(policy.retention.Seconds())
	}
	packetMap := buildRecordedPacket(packet, policy)
	if len(policy.redactFields) > 0 {
		applyRedactions(packetMap, policy.redactFields)
	}
	record["packet"] = packetMap
	return json.Marshal(record)
}

func buildRecordedPacket(packet *transportpb.DataPacket, policy recordingPolicy) map[string]any {
	result := map[string]any{}
	if packet == nil {
		return result
	}
	if packet.Metadata != nil {
		metadataCopy := make(map[string]string, len(packet.Metadata))
		maps.Copy(metadataCopy, packet.Metadata)
		result["metadata"] = metadataCopy
	}
	if env := packet.GetEnvelope(); env != nil {
		result["envelope"] = map[string]any{
			"stream_id":   env.GetStreamId(),
			"sequence":    env.GetSequence(),
			"partition":   env.GetPartition(),
			"chunk_id":    env.GetChunkId(),
			"chunk_index": env.GetChunkIndex(),
			"chunk_count": env.GetChunkCount(),
			"chunk_bytes": env.GetChunkBytes(),
			"total_bytes": env.GetTotalBytes(),
		}
	}

	mode := strings.ToLower(strings.TrimSpace(policy.mode))
	if mode == recordingModePayload {
		if packet.GetPayload() != nil {
			result["payload"] = packet.GetPayload().AsMap()
		}
		if packet.GetInputs() != nil {
			result["inputs"] = packet.GetInputs().AsMap()
		}
		if audio := packet.GetAudio(); audio != nil {
			result["audio"] = map[string]any{
				"codec":          audio.GetCodec(),
				"timestamp_ms":   audio.GetTimestampMs(),
				"sample_rate_hz": audio.GetSampleRateHz(),
				"channels":       audio.GetChannels(),
				"pcm":            base64.StdEncoding.EncodeToString(audio.GetPcm()),
			}
		}
		if video := packet.GetVideo(); video != nil {
			result["video"] = map[string]any{
				"codec":        video.GetCodec(),
				"timestamp_ms": video.GetTimestampMs(),
				"width":        video.GetWidth(),
				"height":       video.GetHeight(),
				"raw":          video.GetRaw(),
				"payload":      base64.StdEncoding.EncodeToString(video.GetPayload()),
			}
		}
		if binary := packet.GetBinary(); binary != nil {
			result["binary"] = map[string]any{
				"mime_type":    binary.GetMimeType(),
				"timestamp_ms": binary.GetTimestampMs(),
				"payload":      base64.StdEncoding.EncodeToString(binary.GetPayload()),
			}
		}
		return result
	}

	if audio := packet.GetAudio(); audio != nil {
		result["audio"] = map[string]any{
			"codec":          audio.GetCodec(),
			"timestamp_ms":   audio.GetTimestampMs(),
			"sample_rate_hz": audio.GetSampleRateHz(),
			"channels":       audio.GetChannels(),
			"bytes":          len(audio.GetPcm()),
		}
	}
	if video := packet.GetVideo(); video != nil {
		result["video"] = map[string]any{
			"codec":        video.GetCodec(),
			"timestamp_ms": video.GetTimestampMs(),
			"width":        video.GetWidth(),
			"height":       video.GetHeight(),
			"raw":          video.GetRaw(),
			"bytes":        len(video.GetPayload()),
		}
	}
	if binary := packet.GetBinary(); binary != nil {
		result["binary"] = map[string]any{
			"mime_type":    binary.GetMimeType(),
			"timestamp_ms": binary.GetTimestampMs(),
			"bytes":        len(binary.GetPayload()),
		}
	}
	return result
}

func applyRedactions(packetMap map[string]any, fields []string) {
	if packetMap == nil {
		return
	}
	for _, field := range fields {
		path := strings.TrimSpace(field)
		if path == "" {
			continue
		}
		rel := path
		if after, ok := strings.CutPrefix(rel, "packet."); ok {
			rel = after
		}
		deleteAtPath(packetMap, strings.Split(rel, "."))
	}
}

func deleteAtPath(root map[string]any, path []string) {
	if len(path) == 0 {
		return
	}
	current := root
	for i := 0; i < len(path)-1; i++ {
		segment := strings.TrimSpace(path[i])
		if segment == "" {
			return
		}
		value, ok := current[segment]
		if !ok {
			return
		}
		next, ok := value.(map[string]any)
		if !ok {
			return
		}
		current = next
	}
	last := strings.TrimSpace(path[len(path)-1])
	if last == "" {
		return
	}
	delete(current, last)
}

//nolint:gocyclo // Retention GC combines policy checks, rate limiting, and storage cleanup in one coordinator path.
func maybeEnforceRecordingRetention(ctx context.Context, store *storage.StorageManager, storyRunName, storyRunNS, stepID string, policy recordingPolicy) {
	if store == nil || policy.retention <= 0 {
		return
	}
	if recordingRetentionPolicy.GCInterval <= 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	prefix := recordingStoragePrefix(storyRunNS, storyRunName, stepID)
	if prefix == "" {
		return
	}
	if !shouldRunRecordingGC(prefix) {
		return
	}

	paths, err := store.List(ctx, prefix)
	if err != nil {
		if errors.Is(err, storage.ErrUnsupportedOperation) {
			metrics.RecordHubRecording(storyRunName, stepID, policy.mode, "gc_unsupported")
			return
		}
		metrics.RecordHubRecording(storyRunName, stepID, policy.mode, "gc_error")
		return
	}
	if len(paths) == 0 {
		return
	}

	cutoff := time.Now().Add(-policy.retention)
	candidates := recordingGCCandidates(paths, cutoff, recordingRetentionPolicy.MaxScan)
	if len(candidates) == 0 {
		return
	}

	maxDelete := recordingRetentionPolicy.MaxDelete
	deleted := 0
	for _, candidate := range candidates {
		if maxDelete > 0 && deleted >= maxDelete {
			break
		}
		if err := store.Delete(ctx, candidate.path); err != nil && !storage.IsNotFound(err) {
			metrics.RecordHubRecording(storyRunName, stepID, policy.mode, "gc_delete_error")
			continue
		}
		deleted++
	}
	if deleted > 0 {
		metrics.RecordHubRecording(storyRunName, stepID, policy.mode, "gc_deleted")
	}
}

func shouldRunRecordingGC(prefix string) bool {
	now := time.Now()
	gcInterval := recordingRetentionPolicy.GCInterval
	recordingGCLock.Lock()
	defer recordingGCLock.Unlock()
	if last, ok := recordingGCLastRun[prefix]; ok {
		if now.Sub(last) < gcInterval {
			return false
		}
	}
	recordingGCLastRun[prefix] = now
	// Prune stale entries to prevent unbounded map growth.
	if gcInterval > 0 {
		cutoff := now.Add(-2 * gcInterval)
		for k, lastRun := range recordingGCLastRun {
			if lastRun.Before(cutoff) {
				delete(recordingGCLastRun, k)
			}
		}
	}
	return true
}

type recordingGCCandidate struct {
	path      string
	timestamp time.Time
}

func recordingGCCandidates(paths []string, cutoff time.Time, maxScan int) []recordingGCCandidate {
	if len(paths) == 0 {
		return nil
	}
	ordered := append([]string(nil), paths...)
	sort.Strings(ordered)
	if maxScan > 0 && len(ordered) > maxScan {
		ordered = ordered[:maxScan]
	}
	out := make([]recordingGCCandidate, 0, len(ordered))
	for _, path := range ordered {
		ts, ok := parseRecordingTimestamp(path)
		if !ok {
			continue
		}
		if ts.Before(cutoff) {
			out = append(out, recordingGCCandidate{path: path, timestamp: ts})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].timestamp.Before(out[j].timestamp)
	})
	return out
}

func parseRecordingTimestamp(path string) (time.Time, bool) {
	base := filepath.Base(path)
	if base == "" {
		return time.Time{}, false
	}
	dash := strings.Index(base, "-")
	if dash <= 0 {
		return time.Time{}, false
	}
	ts := base[:dash]
	parsed, err := time.Parse(recordingTimestampFormat, ts)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func recordingStoragePrefix(namespace, storyRunName, stepID string) string {
	parts := []string{"stream-recordings"}
	if strings.TrimSpace(namespace) != "" {
		parts = append(parts, namespace)
	}
	if strings.TrimSpace(storyRunName) != "" {
		parts = append(parts, storyRunName)
	}
	if strings.TrimSpace(stepID) != "" {
		parts = append(parts, stepID)
	}
	return filepath.ToSlash(filepath.Join(parts...))
}

func recordingStoragePath(namespace, storyRunName, stepID string) string {
	prefix := recordingStoragePrefix(namespace, storyRunName, stepID)
	fileName := fmt.Sprintf("%s-%s.json", time.Now().UTC().Format(recordingTimestampFormat), randomHex(6))
	if prefix == "" {
		return fileName
	}
	return filepath.ToSlash(filepath.Join(prefix, fileName))
}

func randomHex(bytesLen int) string {
	if bytesLen <= 0 {
		bytesLen = 6
	}
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err == nil {
		return hex.EncodeToString(buf)
	}
	max := big.NewInt(1)
	max.Lsh(max, uint(bytesLen*8))
	val, err := rand.Int(rand.Reader, max)
	if err == nil {
		return fmt.Sprintf("%x", val.Uint64())
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func recordEventTimeMetrics(storyRunName, stepID string, eventTime time.Time) {
	lag := max(time.Since(eventTime), time.Duration(0))
	metrics.RecordHubEventTime(storyRunName, stepID, eventTime, lag)
}
