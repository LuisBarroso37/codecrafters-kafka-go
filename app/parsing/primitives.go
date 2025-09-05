package parsing

import (
	"encoding/binary"
	"fmt"
)

func ExtractInt16(buffer []byte, index int) (int16, int, error) {
	if index+2 > len(buffer) {
		return 0, index, fmt.Errorf("failed to extract int16 - buffer too small")
	}

	value := int16(binary.BigEndian.Uint16(buffer[index : index+2]))
	return value, index + 2, nil
}

func ExtractInt32(buffer []byte, index int) (int32, int, error) {
	if index+4 > len(buffer) {
		return 0, index, fmt.Errorf("failed to extract int32 - buffer too small")
	}

	value := int32(binary.BigEndian.Uint32(buffer[index : index+4]))
	return value, index + 4, nil
}

func ExtractNullableString(buffer []byte, index int) (string, int, error) {
	length, index, err := ExtractInt16(buffer, index)
	if err != nil {
		return "", index, err
	}

	if length == -1 {
		return "", index, nil
	}

	if index+int(length) > len(buffer) {
		return "", index, fmt.Errorf("failed to extract nullable string - buffer too small")
	}

	value := string(buffer[index : index+int(length)])
	return value, index + int(length), nil
}

func ExtractUnsignedVarInt(buffer []byte, index int) (uint64, int, error) {
	value, bytesRead := binary.Uvarint(buffer[index:])

	if bytesRead == 0 {
		return 0, index, fmt.Errorf("failed to extract unsigned varint - buffer too small")
	}

	if bytesRead < 0 {
		return 0, index, fmt.Errorf("failed to extract unsigned varint - invalid encoding")
	}

	return value, index + bytesRead, nil
}

func ExtractCompactString(buffer []byte, index int) (string, int, error) {
	length, index, err := ExtractUnsignedVarInt(buffer, index)
	if err != nil {
		return "", index, err
	}

	if length == 0 {
		return "", index, fmt.Errorf("invalid compact string length")
	}

	if length == 1 {
		return "", index, nil
	}

	numberOfBytesToRead := length - 1 // Unsigned varint represents the length N + 1 bytes for the following string

	if index+int(numberOfBytesToRead) > len(buffer) {
		return "", index, fmt.Errorf("failed to extract compact string - buffer too small")
	}

	value := string(buffer[index : index+int(numberOfBytesToRead)])
	return value, index + int(numberOfBytesToRead), nil
}
