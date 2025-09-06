package serializer

import (
	"encoding/binary"
	"fmt"
)

func SerializeInt8(buffer []byte, index int, value int8) (int, error) {
	if index < 0 {
		return index, fmt.Errorf("failed to serialize int8 - negative index")
	}

	if index+1 > len(buffer) {
		return index, fmt.Errorf("failed to serialize int8 - buffer too small")
	}

	buffer[index] = byte(value)
	return index + 1, nil
}

func SerializeInt16(buffer []byte, index int, value int16) (int, error) {
	if index < 0 {
		return index, fmt.Errorf("failed to serialize int16 - negative index")
	}

	if index+2 > len(buffer) {
		return index, fmt.Errorf("failed to serialize int16 - buffer too small")
	}

	binary.BigEndian.PutUint16(buffer[index:index+2], uint16(value))
	return index + 2, nil
}

func SerializeInt32(buffer []byte, index int, value int32) (int, error) {
	if index < 0 {
		return index, fmt.Errorf("failed to serialize int32 - negative index")
	}

	if index+4 > len(buffer) {
		return index, fmt.Errorf("failed to serialize int32 - buffer too small")
	}

	binary.BigEndian.PutUint32(buffer[index:index+4], uint32(value))
	return index + 4, nil
}

func SerializeUnsignedVarInt(buffer []byte, index int, value uint64) (int, error) {
	if index < 0 {
		return index, fmt.Errorf("failed to serialize varint - negative index")
	}

	if index+binary.MaxVarintLen64 > len(buffer) {
		return index, fmt.Errorf("failed to serialize varint - buffer too small")
	}

	index += binary.PutUvarint(buffer[index:], value)
	return index, nil
}
