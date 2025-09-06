package serializer

import (
	"encoding/binary"
	"fmt"
)

func SerializeCompactString(buffer []byte, index int, value string) (int, error) {
	stringLen := len(value)
	varintValue := uint64(stringLen + 1)
	varintBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), varintValue)
	totalBytesNeeded := varintBytes + stringLen

	if index+totalBytesNeeded > len(buffer) {
		return 0, fmt.Errorf("failed to serialize compact string - buffer too small")
	}

	// Value length
	index += binary.PutUvarint(buffer[index:], uint64(varintValue))

	// Value (bytes)
	copy(buffer[index:], []byte(value))
	index += stringLen

	return index, nil
}
