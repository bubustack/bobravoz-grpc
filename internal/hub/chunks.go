package hub

import (
	"fmt"
	"strings"
	"time"

	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
	"google.golang.org/protobuf/proto"
)

const (
	defaultChunkReassemblyTTL       = 2 * time.Minute
	defaultChunkMaxInFlightBytes    = 64 * 1024 * 1024
	defaultChunkMaxChunkBytes       = 10 * 1024 * 1024
	defaultChunkMaxPerStreamChunks  = 64
	defaultChunkMaxGlobalAssemblies = 1024
)

type packetChunkReassembler struct {
	ttl              time.Duration
	maxPerStream     int
	maxGlobal        int
	maxBytes         int
	maxChunkBytes    int
	inflightBytes    int
	assemblies       map[string]*packetChunkAssembly
	streamChunkCount map[string]int // per-stream assembly count
}

type packetChunkAssembly struct {
	lastSeen     time.Time
	expected     uint32
	received     uint32
	totalBytes   uint32
	base         *transportpb.DataPacket
	chunks       [][]byte
	receivedByte int
	streamID     string
}

func newPacketChunkReassembler(ttl time.Duration, maxChunks int, maxBytes int) *packetChunkReassembler {
	if ttl <= 0 {
		ttl = defaultChunkReassemblyTTL
	}
	if maxBytes <= 0 {
		maxBytes = defaultChunkMaxInFlightBytes
	}
	maxGlobal := defaultChunkMaxGlobalAssemblies
	if maxChunks > 0 && maxChunks < maxGlobal {
		maxGlobal = maxChunks
	}
	return &packetChunkReassembler{
		ttl:              ttl,
		maxPerStream:     defaultChunkMaxPerStreamChunks,
		maxGlobal:        maxGlobal,
		maxBytes:         maxBytes,
		maxChunkBytes:    defaultChunkMaxChunkBytes,
		assemblies:       make(map[string]*packetChunkAssembly),
		streamChunkCount: make(map[string]int),
	}
}

//nolint:gocyclo // Chunk reassembly validates sequence, size, expiry, and completion in one hot-path function.
func (r *packetChunkReassembler) Add(packet *transportpb.DataPacket) (*transportpb.DataPacket, bool, error) {
	if r == nil || packet == nil {
		return nil, false, nil
	}
	env := packet.GetEnvelope()
	if !isChunkedEnvelope(env) {
		return packet, true, nil
	}
	if env == nil {
		return nil, false, fmt.Errorf("chunked packet missing envelope")
	}
	chunkID := strings.TrimSpace(env.GetChunkId())
	if chunkID == "" {
		return nil, false, fmt.Errorf("chunked packet missing chunk_id")
	}
	streamID := strings.TrimSpace(env.GetStreamId())
	if streamID == "" {
		return nil, false, fmt.Errorf("chunked packet missing stream_id")
	}
	count := env.GetChunkCount()
	index := env.GetChunkIndex()
	if count == 0 {
		return nil, false, fmt.Errorf("chunked packet missing chunk_count")
	}
	if index >= count {
		return nil, false, fmt.Errorf("chunk_index %d out of range (count %d)", index, count)
	}
	binary := packet.GetBinary()
	if binary == nil {
		return nil, false, fmt.Errorf("chunked packet missing binary frame")
	}
	payload := binary.GetPayload()
	if r.maxChunkBytes > 0 && len(payload) > r.maxChunkBytes {
		return nil, false, fmt.Errorf("chunk payload %d exceeds max chunk size %d", len(payload), r.maxChunkBytes)
	}
	if env.GetChunkBytes() > 0 && int(env.GetChunkBytes()) != len(payload) {
		return nil, false, fmt.Errorf("chunk_bytes %d does not match payload size %d", env.GetChunkBytes(), len(payload))
	}

	key := chunkKey(streamID, env.GetPartition(), chunkID)
	now := time.Now()
	r.evictExpired(now)

	assembly := r.assemblies[key]
	if assembly == nil {
		if r.maxGlobal > 0 && len(r.assemblies) >= r.maxGlobal {
			return nil, false, fmt.Errorf("too many chunk assemblies in flight (global limit %d)", r.maxGlobal)
		}
		if r.maxPerStream > 0 && r.streamChunkCount[streamID] >= r.maxPerStream {
			return nil, false, fmt.Errorf("too many chunk assemblies for stream %q (limit %d)", streamID, r.maxPerStream)
		}
		if r.inflightBytes+len(payload) > r.maxBytes {
			return nil, false, fmt.Errorf("chunk reassembly buffer exceeded (%d > %d)", r.inflightBytes+len(payload), r.maxBytes)
		}
		assembly = &packetChunkAssembly{
			lastSeen:   now,
			expected:   count,
			totalBytes: env.GetTotalBytes(),
			base:       proto.Clone(packet).(*transportpb.DataPacket),
			chunks:     make([][]byte, count),
			streamID:   streamID,
		}
		r.assemblies[key] = assembly
		r.streamChunkCount[streamID]++
	}

	if assembly.expected != count {
		return nil, false, fmt.Errorf("chunk_count mismatch for %s: %d != %d", chunkID, count, assembly.expected)
	}
	// Check for duplicate chunk before any byte accounting.
	if assembly.chunks[index] != nil {
		assembly.lastSeen = now
		return nil, false, nil
	}
	// Check inflight capacity for existing assemblies (new assemblies are
	// checked above when created).
	if r.inflightBytes+len(payload) > r.maxBytes {
		return nil, false, fmt.Errorf("chunk reassembly buffer exceeded (%d > %d)", r.inflightBytes+len(payload), r.maxBytes)
	}
	assembly.chunks[index] = append([]byte(nil), payload...)
	assembly.received++
	assembly.receivedByte += len(payload)
	assembly.lastSeen = now
	r.inflightBytes += len(payload)

	if assembly.received < assembly.expected {
		return nil, false, nil
	}

	assembled, err := reassemblePacket(assembly)
	if err != nil {
		r.removeAssembly(key, assembly)
		return nil, false, err
	}
	r.removeAssembly(key, assembly)
	return assembled, true, nil
}

func reassemblePacket(assembly *packetChunkAssembly) (*transportpb.DataPacket, error) {
	if assembly == nil || assembly.base == nil {
		return nil, fmt.Errorf("chunk assembly missing base packet")
	}
	payloadLen := 0
	for i := uint32(0); i < assembly.expected; i++ {
		chunk := assembly.chunks[i]
		if chunk == nil {
			return nil, fmt.Errorf("chunk %d missing", i)
		}
		payloadLen += len(chunk)
	}
	if assembly.totalBytes > 0 && payloadLen != int(assembly.totalBytes) {
		return nil, fmt.Errorf("total_bytes %d does not match assembled payload %d", assembly.totalBytes, payloadLen)
	}
	payload := make([]byte, 0, payloadLen)
	for _, chunk := range assembly.chunks {
		payload = append(payload, chunk...)
	}
	cloned := proto.Clone(assembly.base).(*transportpb.DataPacket)
	binary := cloned.GetBinary()
	if binary == nil {
		return nil, fmt.Errorf("chunked packet missing binary frame")
	}
	binary.Payload = payload
	clearChunkFields(cloned.GetEnvelope())
	return cloned, nil
}

func (r *packetChunkReassembler) removeAssembly(key string, assembly *packetChunkAssembly) {
	if assembly == nil {
		delete(r.assemblies, key)
		return
	}
	delete(r.assemblies, key)
	r.inflightBytes -= assembly.receivedByte
	if assembly.streamID != "" {
		r.streamChunkCount[assembly.streamID]--
		if r.streamChunkCount[assembly.streamID] <= 0 {
			delete(r.streamChunkCount, assembly.streamID)
		}
	}
}

func (r *packetChunkReassembler) evictExpired(now time.Time) {
	if r == nil || r.ttl <= 0 {
		return
	}
	for key, assembly := range r.assemblies {
		if assembly == nil {
			delete(r.assemblies, key)
			continue
		}
		if now.Sub(assembly.lastSeen) > r.ttl {
			r.removeAssembly(key, assembly)
		}
	}
}

func isChunkedEnvelope(env *transportpb.StreamEnvelope) bool {
	if env == nil {
		return false
	}
	return env.GetChunkId() != "" || env.GetChunkCount() > 0 || env.GetChunkIndex() > 0 || env.GetChunkBytes() > 0 || env.GetTotalBytes() > 0
}

func chunkKey(streamID, partition, chunkID string) string {
	if partition == "" {
		return streamID + "|" + chunkID
	}
	return streamID + "|" + partition + "|" + chunkID
}

func clearChunkFields(env *transportpb.StreamEnvelope) {
	if env == nil {
		return
	}
	env.ChunkId = ""
	env.ChunkIndex = 0
	env.ChunkCount = 0
	env.ChunkBytes = 0
	env.TotalBytes = 0
}
