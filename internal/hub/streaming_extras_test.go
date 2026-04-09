package hub

import (
	"strconv"
	"testing"
	"time"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
)

func TestExtractEventTimeUsesCanonicalEnvelopeMetadataKey(t *testing.T) {
	ts := time.UnixMilli(1700000000123).UTC()
	packet := &transportpb.DataPacket{
		Metadata: map[string]string{
			metadataEnvelopeTimeKey: strconv.FormatInt(ts.UnixMilli(), 10),
			"timestamp_ms":          strconv.FormatInt(ts.Add(time.Second).UnixMilli(), 10),
		},
	}

	got, ok := extractEventTime(packet, "")
	if !ok {
		t.Fatal("expected canonical envelope timestamp to be extracted")
	}
	if !got.Equal(ts) {
		t.Fatalf("expected %s, got %s", ts, got)
	}
}

func TestExtractEventTimeIgnoresLegacyMetadataTimestampKeysByDefault(t *testing.T) {
	packet := &transportpb.DataPacket{
		Metadata: map[string]string{
			"event_time_ms": "1700000000123",
			"event-time-ms": "1700000000123",
			"timestamp_ms":  "1700000000123",
			"timestamp":     "1700000000123",
		},
	}

	got, ok := extractEventTime(packet, "")
	if ok {
		t.Fatalf("expected legacy metadata timestamp keys to be ignored, got %s", got)
	}
	if !got.IsZero() {
		t.Fatalf("expected zero timestamp when only legacy keys are present, got %s", got)
	}
}
