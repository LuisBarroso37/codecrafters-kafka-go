package serializer

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// SerializeUUID serializes a UUID string to 16 bytes for Kafka protocol
// Input: "550e8400-e29b-41d4-a716-446655440000" (36 chars with dashes)
// Output: 16 raw bytes
// A UUID like "550e8400-e29b-41d4-a716-446655440000" is actually a hexadecimal representation of 16 bytes of binary data.
//
// Why hex.DecodeString?
// Each pair of characters represents one byte:
//
// "55" → byte value 85 (0x55)
// "0e" → byte value 14 (0x0e)
// "84" → byte value 132 (0x84)
// ...
func SerializeUUID(buffer []byte, index int, uuidStr string) (int, error) {
	// Remove dashes from UUID string
	cleanUUID := strings.ReplaceAll(uuidStr, "-", "")

	// Convert hex string to bytes
	uuidBytes, err := hex.DecodeString(cleanUUID)
	if err != nil {
		return index, fmt.Errorf("failed to decode UUID hex string: %v", err)
	}

	if len(uuidBytes) != 16 {
		return 0, fmt.Errorf("failed to serialize uuid - invalid length")
	}

	if index+len(uuidBytes) > len(buffer) {
		return 0, fmt.Errorf("failed to serialize uuid - buffer too small")
	}

	// Copy 16 bytes to buffer
	copy(buffer[index:index+16], uuidBytes)

	return index + 16, nil
}
