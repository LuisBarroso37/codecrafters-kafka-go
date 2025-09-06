package parser

import (
	"fmt"
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
