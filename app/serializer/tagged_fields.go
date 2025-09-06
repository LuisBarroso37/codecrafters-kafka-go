package serializer

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

func SerializeTaggedFields(buffer []byte, index int, taggedFields map[string]string) (int, error) {
	if index < 0 {
		return index, fmt.Errorf("failed to serialize tagged fields - negative index")
	}

	// Check if we have space for at least the length varint
	lengthVarintBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), uint64(len(taggedFields)))
	if index+lengthVarintBytes > len(buffer) {
		return index, fmt.Errorf("failed to serialize tagged fields - buffer too small for length")
	}

	// Number of tagged fields
	bytesWritten := binary.PutUvarint(buffer[index:], uint64(len(taggedFields)))
	index += bytesWritten

	// Each tagged field
	for tagIDStr, value := range taggedFields {
		tagID, err := strconv.ParseUint(tagIDStr, 10, 32)
		if err != nil {
			return index, fmt.Errorf("failed to parse tag ID %q: %v", tagIDStr, err)
		}

		// Calculate space needed for this field
		tagVarintBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), tagID)
		valueVarintBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), uint64(len(value)+1))
		totalFieldBytes := tagVarintBytes + valueVarintBytes + len(value)

		if index+totalFieldBytes > len(buffer) {
			return index, fmt.Errorf("failed to serialize tagged fields - buffer too small for field")
		}

		// Tag ID
		bytesWritten = binary.PutUvarint(buffer[index:], tagID)
		index += bytesWritten

		// Value
		index, err = SerializeCompactString(buffer, index, value)
		if err != nil {
			return index, err
		}
	}

	return index, nil
}
