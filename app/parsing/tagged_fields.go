package parsing

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

func ExtractTagFields(buffer []byte, index int) (map[string]string, int, error) {
	taggedFields := make(map[string]string)

	taggedFieldsLength, newIndex, err := ExtractUnsignedVarInt(buffer, index)
	index = newIndex
	if err != nil {
		return taggedFields, index, err
	}

	for i := 0; i < int(taggedFieldsLength); i++ {
		fieldTag, newIndex, err := ExtractUnsignedVarInt(buffer, index)
		if err != nil {
			return taggedFields, newIndex, err
		}

		index = newIndex

		value, newIndex, err := ExtractCompactString(buffer, index)
		if err != nil {
			return nil, newIndex, err
		}
		index = newIndex

		taggedFields[fmt.Sprintf("%d", fieldTag)] = value
	}

	return taggedFields, index, nil
}

func SerializeTaggedFields(buffer []byte, index int, taggedFields map[string]string) (int, error) {
	// 1. Number of tagged fields (unsigned varint)
	index += binary.PutUvarint(buffer[index:], uint64(len(taggedFields)))

	// 2. Each tagged field
	for tagIDStr, value := range taggedFields {
		tagID, err := strconv.ParseUint(tagIDStr, 10, 32)
		if err != nil {
			return 0, err
		}

		// Tag ID (unsigned varint)
		index += binary.PutUvarint(buffer[index:], tagID)

		// Value length (unsigned varint)
		index += binary.PutUvarint(buffer[index:], uint64(len(value)))

		// Value (bytes)
		copy(buffer[index:], []byte(value))
		index += len(value)
	}

	return index, nil
}
