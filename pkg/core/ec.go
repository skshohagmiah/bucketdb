package core

import (
	"errors"
)

// ErasureCoder handles data splitting and reconstruction
type ErasureCoder struct {
	DataShards   int
	ParityShards int
}

// NewErasureCoder creates a new coder
func NewErasureCoder(dataShards, parityShards int) *ErasureCoder {
	return &ErasureCoder{
		DataShards:   dataShards,
		ParityShards: parityShards,
	}
}

// Encode splits data into shards and generates parity
// For 2+1, this means: Shard1, Shard2, Parity(XOR)
func (ec *ErasureCoder) Encode(data []byte) ([][]byte, error) {
	if ec.DataShards != 2 || ec.ParityShards != 1 {
		return nil, errors.New("currently only 2+1 erasure coding is supported in prototype")
	}

	totalLen := len(data)
	shardSize := (totalLen + ec.DataShards - 1) / ec.DataShards

	// Create shards
	shards := make([][]byte, ec.DataShards+ec.ParityShards)
	for i := range shards {
		shards[i] = make([]byte, shardSize)
	}

	// Copy data to shards
	for i := 0; i < totalLen; i++ {
		shardIdx := i / shardSize
		byteIdx := i % shardSize
		shards[shardIdx][byteIdx] = data[i]
	}

	// Calculate parity (XOR of all data shards)
	parityShard := shards[ec.DataShards]
	for i := 0; i < shardSize; i++ {
		var b byte
		for j := 0; j < ec.DataShards; j++ {
			b ^= shards[j][i]
		}
		parityShard[i] = b
	}

	return shards, nil
}

// Decode reconstructs data from shards
// shards argument should contain available shards, with nil for missing ones
func (ec *ErasureCoder) Decode(shards [][]byte, size int) ([]byte, error) {
	if ec.DataShards != 2 || ec.ParityShards != 1 {
		return nil, errors.New("currently only 2+1 erasure coding is supported in prototype")
	}

	// Identify missing shards
	missingIdx := -1
	for i := 0; i < ec.DataShards+ec.ParityShards; i++ {
		if len(shards[i]) == 0 {
			if missingIdx != -1 {
				return nil, errors.New("too many missing shards, cannot reconstruct")
			}
			missingIdx = i
		}
	}

	if missingIdx == -1 {
		// All shards present, just join data shards
		return ec.joinShards(shards[:ec.DataShards], size), nil
	}

	// Reconstruct logic
	shardSize := len(shards[(missingIdx+1)%3]) // Get size from a present shard
	recovered := make([]byte, shardSize)

	// In XOR parity (A ^ B = P), any missing element is XOR of the other two.
	// A = P ^ B
	// B = P ^ A
	// P = A ^ B

	// Find the other two present shards
	var present [][]byte
	for i := 0; i < 3; i++ {
		if i != missingIdx {
			present = append(present, shards[i])
		}
	}

	for i := 0; i < shardSize; i++ {
		recovered[i] = present[0][i] ^ present[1][i]
	}

	// Restore the shards array
	shards[missingIdx] = recovered

	return ec.joinShards(shards[:ec.DataShards], size), nil
}

func (ec *ErasureCoder) joinShards(dataShards [][]byte, originalSize int) []byte {
	result := make([]byte, 0, originalSize)
	/*
		The original implementation logic had a flaw here.
		Wait, simpler:
		Data: [A B C D]
		Shard1: [A B]
		Shard2: [C D]

		To join: Just append Shard1 + Shard2.
	*/

	for _, shard := range dataShards {
		result = append(result, shard...)
	}

	if len(result) > originalSize {
		return result[:originalSize]
	}
	return result
}
