package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestReplayPartitionBucketUsesBoundedLabels(t *testing.T) {
	if got := replayPartitionBucket(""); got != "default" {
		t.Fatalf("expected empty partition to map to default bucket, got %q", got)
	}

	inputs := []string{
		"alpha",
		"beta",
		"gamma",
		"tenant-a:partition-123",
		strings.Repeat("x", 256),
	}

	seen := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		got := replayPartitionBucket(input)
		if !strings.HasPrefix(got, "bucket-") {
			t.Fatalf("expected bucket label for %q, got %q", input, got)
		}
		if strings.Contains(got, input) {
			t.Fatalf("expected partition bucket %q to avoid raw partition text %q", got, input)
		}
		seen[got] = struct{}{}
	}

	if len(seen) > replayPartitionBucketCount {
		t.Fatalf("expected at most %d buckets, got %d", replayPartitionBucketCount, len(seen))
	}
}

func TestRecordHubReplayLastAckUsesBucketedMetricLabels(t *testing.T) {
	storyRun := "story"
	step := t.Name()
	partition := "tenant-a"
	bucket := replayPartitionBucket(partition)
	metric := hubReplayLastAckGauge.WithLabelValues(storyRun, step, bucket)

	RecordHubReplayLastAck(storyRun, step, partition, 41)

	assert.Equal(t, 41.0, testutil.ToFloat64(metric))
}

func TestRecordHubStartupCapabilitiesObservation(t *testing.T) {
	mode := "required"
	metric := hubStartupCapabilitiesObservationCounter.WithLabelValues(mode)
	before := testutil.ToFloat64(metric)

	RecordHubStartupCapabilitiesObservation(mode)

	assert.Equal(t, before+1, testutil.ToFloat64(metric))
}
