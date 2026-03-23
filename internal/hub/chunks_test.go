package hub

import (
	"testing"
	"time"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/stretchr/testify/require"
)

func TestPacketChunkReassembler(t *testing.T) {
	reassembler := newPacketChunkReassembler(time.Minute, 0, 0)
	baseEnv := &transportpb.StreamEnvelope{
		StreamId:   "stream-1",
		Sequence:   7,
		Partition:  "p1",
		ChunkId:    "chunk-42",
		ChunkCount: 2,
	}

	packet1 := &transportpb.DataPacket{
		Metadata: map[string]string{"id": "1"},
		Envelope: cloneStreamEnvelope(baseEnv),
		Binary: &transportpb.BinaryFrame{
			Payload:  []byte("foo"),
			MimeType: "application/octet-stream",
		},
	}
	packet1.Envelope.ChunkIndex = 0
	packet1.Envelope.ChunkBytes = uint32(len(packet1.GetBinary().GetPayload()))

	out, complete, err := reassembler.Add(packet1)
	require.NoError(t, err)
	require.False(t, complete)
	require.Nil(t, out)

	packet2 := &transportpb.DataPacket{
		Metadata: map[string]string{"id": "1"},
		Envelope: cloneStreamEnvelope(baseEnv),
		Binary: &transportpb.BinaryFrame{
			Payload:  []byte("bar"),
			MimeType: "application/octet-stream",
		},
	}
	packet2.Envelope.ChunkIndex = 1
	packet2.Envelope.ChunkBytes = uint32(len(packet2.GetBinary().GetPayload()))

	out, complete, err = reassembler.Add(packet2)
	require.NoError(t, err)
	require.True(t, complete)
	require.NotNil(t, out)

	binary := out.GetBinary()
	require.NotNil(t, binary)
	require.Equal(t, []byte("foobar"), binary.GetPayload())

	env := out.GetEnvelope()
	require.NotNil(t, env)
	require.Equal(t, "stream-1", env.GetStreamId())
	require.Equal(t, uint64(7), env.GetSequence())
	require.Equal(t, "p1", env.GetPartition())
	require.Empty(t, env.GetChunkId())
	require.Equal(t, uint32(0), env.GetChunkCount())
	require.Equal(t, uint32(0), env.GetChunkIndex())
}
