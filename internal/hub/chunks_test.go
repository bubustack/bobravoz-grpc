package hub

import (
	"testing"
	"time"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"github.com/stretchr/testify/require"
)

// cloneStreamEnvelope is declared in server.go; tests reuse it directly.

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
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("foo"),
				MimeType: "application/octet-stream",
			},
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
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("bar"),
				MimeType: "application/octet-stream",
			},
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

func TestChunkReassemblyExceedsLimit(t *testing.T) {
	// Create a reassembler with a very small limit
	reassembler := newPacketChunkReassembler(time.Minute, 2, 1024*1024)

	baseEnv := func(chunkID string) *transportpb.StreamEnvelope {
		return &transportpb.StreamEnvelope{
			StreamId:   "stream-1",
			Sequence:   1,
			ChunkId:    chunkID,
			ChunkCount: 2,
		}
	}

	// Add first chunk assembly
	packet1 := &transportpb.DataPacket{
		Envelope: cloneStreamEnvelope(baseEnv("chunk-1")),
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("foo"),
				MimeType: "application/octet-stream",
			},
		},
	}
	packet1.Envelope.ChunkIndex = 0
	packet1.Envelope.ChunkBytes = uint32(len(packet1.GetBinary().GetPayload()))

	_, _, err := reassembler.Add(packet1)
	require.NoError(t, err)

	// Add second chunk assembly
	packet2 := &transportpb.DataPacket{
		Envelope: cloneStreamEnvelope(baseEnv("chunk-2")),
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("bar"),
				MimeType: "application/octet-stream",
			},
		},
	}
	packet2.Envelope.ChunkIndex = 0
	packet2.Envelope.ChunkBytes = uint32(len(packet2.GetBinary().GetPayload()))

	_, _, err = reassembler.Add(packet2)
	require.NoError(t, err)

	// Third chunk assembly should fail - exceeds maxChunks limit of 2
	packet3 := &transportpb.DataPacket{
		Envelope: cloneStreamEnvelope(baseEnv("chunk-3")),
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("baz"),
				MimeType: "application/octet-stream",
			},
		},
	}
	packet3.Envelope.ChunkIndex = 0
	packet3.Envelope.ChunkBytes = uint32(len(packet3.GetBinary().GetPayload()))

	_, _, err = reassembler.Add(packet3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many chunk assemblies")
}

func TestChunkReassemblyDuplicateChunk(t *testing.T) {
	reassembler := newPacketChunkReassembler(time.Minute, 100, 1024*1024)

	baseEnv := &transportpb.StreamEnvelope{
		StreamId:   "stream-1",
		Sequence:   7,
		Partition:  "p1",
		ChunkId:    "chunk-dup",
		ChunkCount: 2,
	}

	// Add first chunk (index 0)
	packet1 := &transportpb.DataPacket{
		Metadata: map[string]string{"id": "1"},
		Envelope: cloneStreamEnvelope(baseEnv),
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("foo"),
				MimeType: "application/octet-stream",
			},
		},
	}
	packet1.Envelope.ChunkIndex = 0
	packet1.Envelope.ChunkBytes = uint32(len(packet1.GetBinary().GetPayload()))

	out, complete, err := reassembler.Add(packet1)
	require.NoError(t, err)
	require.False(t, complete)
	require.Nil(t, out)

	// Send duplicate of first chunk (same index 0)
	packet1Dup := &transportpb.DataPacket{
		Metadata: map[string]string{"id": "1-dup"},
		Envelope: cloneStreamEnvelope(baseEnv),
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("foo-dup"), // Different payload, same chunk index
				MimeType: "application/octet-stream",
			},
		},
	}
	packet1Dup.Envelope.ChunkIndex = 0
	packet1Dup.Envelope.ChunkBytes = uint32(len(packet1Dup.GetBinary().GetPayload()))

	// Duplicate should be handled gracefully - returns (nil, false, nil)
	out, complete, err = reassembler.Add(packet1Dup)
	require.NoError(t, err, "duplicate chunk should not cause an error")
	require.False(t, complete, "duplicate chunk should not complete assembly")
	require.Nil(t, out, "duplicate chunk should not return a packet")

	// Now add the second chunk (index 1) to complete the assembly
	packet2 := &transportpb.DataPacket{
		Metadata: map[string]string{"id": "2"},
		Envelope: cloneStreamEnvelope(baseEnv),
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("bar"),
				MimeType: "application/octet-stream",
			},
		},
	}
	packet2.Envelope.ChunkIndex = 1
	packet2.Envelope.ChunkBytes = uint32(len(packet2.GetBinary().GetPayload()))

	out, complete, err = reassembler.Add(packet2)
	require.NoError(t, err)
	require.True(t, complete)
	require.NotNil(t, out)

	// Verify the original payload was used (not the duplicate)
	binary := out.GetBinary()
	require.NotNil(t, binary)
	require.Equal(t, []byte("foobar"), binary.GetPayload(), "should use original chunk, not duplicate")
}

func TestChunkReassemblyBytesLimitExceeded(t *testing.T) {
	// The byte limit is checked when creating a NEW assembly.
	// A single large chunk that exceeds the byte limit on creation should fail.
	reassembler := newPacketChunkReassembler(time.Minute, 100, 5) // Only 5 bytes max

	baseEnv := &transportpb.StreamEnvelope{
		StreamId:   "stream-1",
		Sequence:   1,
		ChunkId:    "chunk-big",
		ChunkCount: 2,
	}

	// Chunk with 6 bytes — exceeds the 5-byte limit on assembly creation.
	packet1 := &transportpb.DataPacket{
		Envelope: cloneStreamEnvelope(baseEnv),
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("123456"),
				MimeType: "application/octet-stream",
			},
		},
	}
	packet1.Envelope.ChunkIndex = 0
	packet1.Envelope.ChunkBytes = uint32(len(packet1.GetBinary().GetPayload()))

	_, _, err := reassembler.Add(packet1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "buffer exceeded")
}

func TestChunkReassemblyChunkIndexOutOfRange(t *testing.T) {
	reassembler := newPacketChunkReassembler(time.Minute, 100, 1024*1024)

	baseEnv := &transportpb.StreamEnvelope{
		StreamId:   "stream-1",
		Sequence:   1,
		ChunkId:    "chunk-oob",
		ChunkCount: 2,
	}

	// Chunk with index >= count should fail
	packet := &transportpb.DataPacket{
		Envelope: cloneStreamEnvelope(baseEnv),
		Frame: &transportpb.DataPacket_Binary{
			Binary: &transportpb.BinaryFrame{
				Payload:  []byte("foo"),
				MimeType: "application/octet-stream",
			},
		},
	}
	packet.Envelope.ChunkIndex = 5 // Out of range (count is 2)
	packet.Envelope.ChunkBytes = uint32(len(packet.GetBinary().GetPayload()))

	_, _, err := reassembler.Add(packet)
	require.Error(t, err)
	require.Contains(t, err.Error(), "out of range")
}
