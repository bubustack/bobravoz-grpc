package connector

import (
	"encoding/json"
	"testing"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestParseBindingEnvJSON(t *testing.T) {
	info := &transportpb.BindingInfo{
		Driver:      "test-driver",
		Endpoint:    "unix:///tmp/connector.sock",
		AudioCodecs: []string{"pcm16"},
	}
	payload, err := protojson.Marshal(info)
	require.NoError(t, err)
	env := map[string]any{
		"name":      "binding-a",
		"namespace": "default",
		"binding":   json.RawMessage(payload),
	}
	bytes, err := json.Marshal(env)
	require.NoError(t, err)

	ref, err := parseBindingEnv(string(bytes))
	require.NoError(t, err)
	require.Equal(t, "binding-a", ref.Name)
	require.Equal(t, "default", ref.Namespace)
	require.Equal(t, "test-driver", ref.Info.GetDriver())
}
