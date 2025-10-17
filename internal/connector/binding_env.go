package connector

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type bindingReference struct {
	Name      string
	Namespace string
	Info      *transportpb.BindingInfo
	raw       string
}

type bindingEnvelope struct {
	Name      string          `json:"name"`
	Namespace string          `json:"namespace,omitempty"`
	Binding   json.RawMessage `json:"binding"`
	Info      json.RawMessage `json:"info"`
}

func parseBindingEnv(value string) (bindingReference, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return bindingReference{}, fmt.Errorf("transport binding env empty")
	}

	if strings.HasPrefix(value, "{") {
		if ref, err := parseBindingJSON([]byte(value)); err == nil {
			ref.raw = value
			return ref, nil
		} else {
			return bindingReference{}, err
		}
	}

	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return bindingReference{}, fmt.Errorf("decode base64 binding env: %w", err)
	}
	info, err := decodeBindingInfoBinary(decoded)
	if err != nil {
		return bindingReference{}, fmt.Errorf("decode binding info: %w", err)
	}
	return bindingReference{Info: info, raw: value}, nil
}

func parseBindingJSON(data []byte) (bindingReference, error) {
	var env bindingEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return bindingReference{}, fmt.Errorf("unmarshal binding envelope: %w", err)
	}
	ref, err := buildReferenceFromEnvelope(env)
	if err != nil {
		return bindingReference{}, err
	}
	return ref, nil
}

func buildReferenceFromEnvelope(env bindingEnvelope) (bindingReference, error) {
	var blob []byte
	switch {
	case len(env.Binding) > 0:
		blob = env.Binding
	case len(env.Info) > 0:
		blob = env.Info
	default:
		return bindingReference{}, fmt.Errorf("binding envelope missing payload")
	}
	info, err := decodeBindingInfoJSON(blob)
	if err != nil {
		return bindingReference{}, err
	}
	return bindingReference{
		Name:      strings.TrimSpace(env.Name),
		Namespace: strings.TrimSpace(env.Namespace),
		Info:      info,
	}, nil
}

func decodeBindingInfoJSON(data []byte) (*transportpb.BindingInfo, error) {
	var info transportpb.BindingInfo
	if err := protojson.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func decodeBindingInfoBinary(data []byte) (*transportpb.BindingInfo, error) {
	var info transportpb.BindingInfo
	if err := proto.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}
