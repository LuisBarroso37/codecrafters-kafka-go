package serializer

import (
	"encoding/binary"
	"fmt"
)

func SerializeCompactNullableString(buffer []byte, index int, value *string) (int, error) {
	if index < 0 {
		return index, fmt.Errorf("failed to serialize compact nullable string - negative index")
	}

	if value == nil {
		// For null values, we need space for the varint encoding of 0
		varintBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), uint64(0))
		if index+varintBytes > len(buffer) {
			return index, fmt.Errorf("failed to serialize compact nullable string - buffer too small")
		}

		index += binary.PutUvarint(buffer[index:], uint64(0))
		return index, nil
	}

	// For non-null values, we need space for:
	// 1. The varint encoding of (string length + 1)
	// 2. The string content itself
	stringLen := len(*value)
	varintValue := uint64(stringLen + 1)
	varintBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), varintValue)
	totalBytesNeeded := varintBytes + stringLen

	if index+totalBytesNeeded > len(buffer) {
		return index, fmt.Errorf("failed to serialize compact nullable string - buffer too small")
	}

	// Value length
	index += binary.PutUvarint(buffer[index:], varintValue)

	// Value (bytes)
	copy(buffer[index:], []byte(*value))
	index += stringLen

	return index, nil
}
