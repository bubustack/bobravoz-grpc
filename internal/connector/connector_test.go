package connector

import (
	"encoding/json"
	"testing"

	"github.com/bubustack/tractatus/envelope"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPublishRequestAudioRoundTrip(t *testing.T) {
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Audio{Audio: &transportpb.AudioFrame{
			Pcm:          []byte{0x01, 0x02, 0x03},
			SampleRateHz: 16000,
			Channels:     1,
			Codec:        "pcm16",
			TimestampMs:  42,
		}},
	}
	payloadStruct, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	if err != nil {
		t.Fatalf("structpb.NewStruct() error = %v", err)
	}
	inputsStruct, err := structpb.NewStruct(map[string]any{"baz": 1})
	if err != nil {
		t.Fatalf("structpb.NewStruct() error = %v", err)
	}
	req.Metadata = map[string]string{
		metadataEnvelopeKindKey:      "speech",
		metadataEnvelopeMessageIDKey: "audio-1",
		metadataEnvelopeTimeKey:      "42",
		"storyRun":                   "sr-audio",
	}
	req.Payload = payloadStruct
	req.Inputs = inputsStruct
	req.Transports = []*transportpb.TransportDescriptor{
		{Name: "livekit", Kind: "media", Mode: "bi"},
	}

	packet, err := publishRequestToHubPacket(req)
	if err != nil {
		t.Fatalf("publishRequestToHubPacket() error = %v", err)
	}
	if packet.GetAudio() == nil {
		t.Fatalf("expected audio frame on packet")
	}
	if packet.Metadata["storyRun"] != "sr-audio" {
		t.Fatalf("metadata not propagated: %#v", packet.Metadata)
	}
	if packet.Payload == nil || packet.Payload.AsMap()["foo"] != "bar" {
		t.Fatalf("payload not propagated: %#v", packet.Payload)
	}
	if packet.Inputs == nil || packet.Inputs.AsMap()["baz"] != 1.0 {
		t.Fatalf("inputs not propagated: %#v", packet.Inputs)
	}
	if len(packet.Transports) != 1 || packet.Transports[0].GetName() != "livekit" {
		t.Fatalf("transports not propagated: %#v", packet.Transports)
	}

	roundTrip, err := hubPacketToPublishRequest(logr.Discard(), packet)
	if err != nil {
		t.Fatalf("hubPacketToPublishRequest() error = %v", err)
	}
	audioReq, ok := roundTrip.GetFrame().(*transportpb.PublishRequest_Audio)
	if !ok {
		t.Fatalf("expected audio frame, got %T", roundTrip.GetFrame())
	}
	if !proto.Equal(req.GetAudio(), audioReq.Audio) {
		t.Fatalf("audio frame mismatch\nexpected: %v\nactual: %v", req.GetAudio(), audioReq.Audio)
	}
	if got := roundTrip.GetMetadata()["storyRun"]; got != "sr-audio" {
		t.Fatalf("metadata mismatch: got %s", got)
	}
	if roundTrip.GetPayload() == nil || roundTrip.GetPayload().AsMap()["foo"] != "bar" {
		t.Fatalf("payload missing on roundtrip")
	}
	if len(roundTrip.GetTransports()) != 1 || roundTrip.GetTransports()[0].GetName() != "livekit" {
		t.Fatalf("transports missing on roundtrip: %#v", roundTrip.GetTransports())
	}
}

func TestPublishRequestVideoRoundTrip(t *testing.T) {
	req := &transportpb.PublishRequest{
		Frame: &transportpb.PublishRequest_Video{Video: &transportpb.VideoFrame{
			Payload:     []byte{0xde, 0xad, 0xbe, 0xef},
			Codec:       "h264",
			Width:       1920,
			Height:      1080,
			TimestampMs: 9001,
			Raw:         false,
		}},
	}

	packet, err := publishRequestToHubPacket(req)
	if err != nil {
		t.Fatalf("publishRequestToHubPacket() error = %v", err)
	}
	if packet.GetVideo() == nil {
		t.Fatalf("expected video frame on packet")
	}

	roundTrip, err := hubPacketToPublishRequest(logr.Discard(), packet)
	if err != nil {
		t.Fatalf("hubPacketToPublishRequest() error = %v", err)
	}
	videoReq, ok := roundTrip.GetFrame().(*transportpb.PublishRequest_Video)
	if !ok {
		t.Fatalf("expected video frame, got %T", roundTrip.GetFrame())
	}
	if !proto.Equal(req.GetVideo(), videoReq.Video) {
		t.Fatalf("video frame mismatch")
	}
}

func TestPublishRequestBinaryRoundTrip(t *testing.T) {
	payload := json.RawMessage(`{"message":"hello"}`)
	inputs := json.RawMessage(`{"key":"value"}`)
	env := &envelope.Envelope{
		Kind:      "data",
		MessageID: "abc123",
		Metadata: map[string]string{
			"step": "ingest",
		},
		Payload: payload,
		Inputs:  inputs,
		Transports: []envelope.TransportDescriptor{
			{Name: "grpc", Kind: "hub", Mode: "bidirectional"},
		},
	}
	frame, err := envelope.ToBinaryFrame(env)
	if err != nil {
		t.Fatalf("envelope.ToBinaryFrame() error = %v", err)
	}
	frame.TimestampMs = 777

	req := &transportpb.PublishRequest{Frame: &transportpb.PublishRequest_Binary{Binary: frame}}

	packet, err := publishRequestToHubPacket(req)
	if err != nil {
		t.Fatalf("publishRequestToHubPacket() error = %v", err)
	}
	if packet.GetPayload() == nil || packet.GetPayload().AsMap()["message"] != "hello" {
		t.Fatalf("expected payload to be unpacked from envelope: %#v", packet.GetPayload())
	}
	if len(packet.Metadata) == 0 || packet.Metadata["step"] != "ingest" {
		t.Fatalf("metadata missing on packet: %#v", packet.Metadata)
	}

	roundTrip, err := hubPacketToPublishRequest(logr.Discard(), packet)
	if err != nil {
		t.Fatalf("hubPacketToPublishRequest() error = %v", err)
	}
	binReq, ok := roundTrip.GetFrame().(*transportpb.PublishRequest_Binary)
	if !ok {
		t.Fatalf("expected binary frame, got %T", roundTrip.GetFrame())
	}

	envOut, err := envelope.FromBinaryFrame(binReq.Binary)
	if err != nil {
		t.Fatalf("envelope.FromBinaryFrame() error = %v", err)
	}
	if envOut.Kind != env.Kind || envOut.MessageID != env.MessageID {
		t.Fatalf("envelope headers mismatch: got %#v want %#v", envOut, env)
	}
	if string(envOut.Payload) != string(env.Payload) {
		t.Fatalf("payload mismatch: got %s want %s", envOut.Payload, env.Payload)
	}
	if string(envOut.Inputs) != string(env.Inputs) {
		t.Fatalf("inputs mismatch: got %s want %s", envOut.Inputs, env.Inputs)
	}
	if binReq.Binary.GetTimestampMs() != frame.GetTimestampMs() {
		t.Fatalf("timestamp mismatch: got %d want %d", binReq.Binary.GetTimestampMs(), frame.GetTimestampMs())
	}
}
