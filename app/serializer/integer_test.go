package serializer

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestSerializeInt8(t *testing.T) {
	tests := []struct {
		name       string
		buffer     []byte
		index      int
		value      int8
		wantIdx    int
		wantErr    bool
		wantBuffer []byte
	}{
		{
			name:       "Valid int8 positive value",
			buffer:     make([]byte, 5),
			index:      0,
			value:      42,
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: []byte{42, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Zero value",
			buffer:     make([]byte, 5),
			index:      0,
			value:      0,
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Max int8 value",
			buffer:     make([]byte, 5),
			index:      0,
			value:      127, // 0x7F
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: []byte{0x7F, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Negative int8 value",
			buffer:     make([]byte, 5),
			index:      0,
			value:      -1, // 0xFF
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: []byte{0xFF, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Min int8 value",
			buffer:     make([]byte, 5),
			index:      0,
			value:      -128, // 0x80
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: []byte{0x80, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Serialization from middle index",
			buffer:     make([]byte, 10),
			index:      3,
			value:      100,
			wantIdx:    4,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x00, 100, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Cursor null value (-1)",
			buffer:     make([]byte, 5),
			index:      0,
			value:      -1, // Used for null cursor
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: []byte{0xFF, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Cursor present value (1)",
			buffer:     make([]byte, 5),
			index:      0,
			value:      1, // Used for non-null cursor
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: []byte{0x01, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:    "Negative index",
			buffer:  make([]byte, 5),
			index:   -1,
			value:   100,
			wantIdx: -1,
			wantErr: true,
		},
		{
			name:    "Buffer too small",
			buffer:  make([]byte, 0),
			index:   0,
			value:   100,
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index at buffer boundary",
			buffer:  make([]byte, 5),
			index:   5,
			value:   100,
			wantIdx: 5,
			wantErr: true,
		},
		{
			name:    "Index beyond buffer",
			buffer:  make([]byte, 5),
			index:   6,
			value:   100,
			wantIdx: 6,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the original buffer to compare
			originalBuffer := make([]byte, len(tt.buffer))
			copy(originalBuffer, tt.buffer)

			gotIdx, err := SerializeInt8(tt.buffer, tt.index, tt.value)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if gotIdx != tt.wantIdx {
				t.Errorf("SerializeInt8() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}

			if tt.wantBuffer != nil && !bytes.Equal(tt.buffer, tt.wantBuffer) {
				t.Errorf("SerializeInt8() buffer = %v, want %v", tt.buffer, tt.wantBuffer)
			}

			// Verify that the value can be read back correctly
			if !tt.wantErr && tt.wantBuffer != nil {
				readValue := int8(tt.buffer[tt.index])
				if readValue != tt.value {
					t.Errorf("SerializeInt8() read back value = %v, want %v", readValue, tt.value)
				}
			}
		})
	}
}

func TestSerializeInt16(t *testing.T) {
	tests := []struct {
		name       string
		buffer     []byte
		index      int
		value      int16
		wantIdx    int
		wantErr    bool
		wantBuffer []byte
	}{
		{
			name:       "Valid int16 serialization",
			buffer:     make([]byte, 10),
			index:      0,
			value:      1234,
			wantIdx:    2,
			wantErr:    false,
			wantBuffer: []byte{0x04, 0xD2, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // 1234 in big endian
		},
		{
			name:       "Zero value",
			buffer:     make([]byte, 5),
			index:      0,
			value:      0,
			wantIdx:    2,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Max int16 value",
			buffer:     make([]byte, 5),
			index:      0,
			value:      32767, // 0x7FFF
			wantIdx:    2,
			wantErr:    false,
			wantBuffer: []byte{0x7F, 0xFF, 0x00, 0x00, 0x00},
		},
		{
			name:       "Negative int16 value",
			buffer:     make([]byte, 5),
			index:      0,
			value:      -1, // 0xFFFF when converted to uint16
			wantIdx:    2,
			wantErr:    false,
			wantBuffer: []byte{0xFF, 0xFF, 0x00, 0x00, 0x00},
		},
		{
			name:       "Min int16 value",
			buffer:     make([]byte, 5),
			index:      0,
			value:      -32768, // 0x8000 when converted to uint16
			wantIdx:    2,
			wantErr:    false,
			wantBuffer: []byte{0x80, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Serialization from middle index",
			buffer:     make([]byte, 10),
			index:      3,
			value:      300,
			wantIdx:    5,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x00, 0x01, 0x2C, 0x00, 0x00, 0x00, 0x00, 0x00}, // 300 in big endian at index 3
		},
		{
			name:    "Negative index",
			buffer:  make([]byte, 5),
			index:   -1,
			value:   100,
			wantIdx: -1,
			wantErr: true,
		},
		{
			name:    "Buffer too small",
			buffer:  make([]byte, 1),
			index:   0,
			value:   100,
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index at buffer boundary",
			buffer:  make([]byte, 5),
			index:   4,
			value:   100,
			wantIdx: 4,
			wantErr: true,
		},
		{
			name:    "Index beyond buffer",
			buffer:  make([]byte, 5),
			index:   5,
			value:   100,
			wantIdx: 5,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the original buffer to compare
			originalBuffer := make([]byte, len(tt.buffer))
			copy(originalBuffer, tt.buffer)

			gotIdx, err := SerializeInt16(tt.buffer, tt.index, tt.value)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if gotIdx != tt.wantIdx {
				t.Errorf("SerializeUint16() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}

			if tt.wantBuffer != nil && !bytes.Equal(tt.buffer, tt.wantBuffer) {
				t.Errorf("SerializeUint16() buffer = %v, want %v", tt.buffer, tt.wantBuffer)
			}
		})
	}
}

func TestSerializeInt32(t *testing.T) {
	tests := []struct {
		name       string
		buffer     []byte
		index      int
		value      int32
		wantIdx    int
		wantErr    bool
		wantBuffer []byte
	}{
		{
			name:       "Valid int32 serialization",
			buffer:     make([]byte, 10),
			index:      0,
			value:      123456789,
			wantIdx:    4,
			wantErr:    false,
			wantBuffer: []byte{0x07, 0x5B, 0xCD, 0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // 123456789 in big endian
		},
		{
			name:       "Zero value",
			buffer:     make([]byte, 8),
			index:      0,
			value:      0,
			wantIdx:    4,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Max int32 value",
			buffer:     make([]byte, 8),
			index:      0,
			value:      2147483647, // 0x7FFFFFFF
			wantIdx:    4,
			wantErr:    false,
			wantBuffer: []byte{0x7F, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Negative int32 value",
			buffer:     make([]byte, 8),
			index:      0,
			value:      -1, // 0xFFFFFFFF when converted to uint32
			wantIdx:    4,
			wantErr:    false,
			wantBuffer: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Min int32 value",
			buffer:     make([]byte, 8),
			index:      0,
			value:      -2147483648, // 0x80000000 when converted to uint32
			wantIdx:    4,
			wantErr:    false,
			wantBuffer: []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Serialization from middle index",
			buffer:     make([]byte, 10),
			index:      2,
			value:      1000000,
			wantIdx:    6,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x00, 0x0F, 0x42, 0x40, 0x00, 0x00, 0x00, 0x00}, // 1000000 in big endian at index 2
		},
		{
			name:    "Negative index",
			buffer:  make([]byte, 8),
			index:   -1,
			value:   100,
			wantIdx: -1,
			wantErr: true,
		},
		{
			name:    "Buffer too small",
			buffer:  make([]byte, 3),
			index:   0,
			value:   100,
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index at buffer boundary",
			buffer:  make([]byte, 8),
			index:   5,
			value:   100,
			wantIdx: 5,
			wantErr: true,
		},
		{
			name:    "Index beyond buffer",
			buffer:  make([]byte, 8),
			index:   8,
			value:   100,
			wantIdx: 8,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the original buffer to compare
			originalBuffer := make([]byte, len(tt.buffer))
			copy(originalBuffer, tt.buffer)

			gotIdx, err := SerializeInt32(tt.buffer, tt.index, tt.value)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if gotIdx != tt.wantIdx {
				t.Errorf("SerializeUint32() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}

			if tt.wantBuffer != nil && !bytes.Equal(tt.buffer, tt.wantBuffer) {
				t.Errorf("SerializeUint32() buffer = %v, want %v", tt.buffer, tt.wantBuffer)
			}
		})
	}
}

func TestSerializeUnsignedVarInt(t *testing.T) {
	tests := []struct {
		name       string
		buffer     []byte
		index      int
		value      uint64
		wantIdx    int
		wantErr    bool
		wantBuffer []byte
	}{
		{
			name:       "Small value (single byte)",
			buffer:     make([]byte, 15),
			index:      0,
			value:      42,
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: append([]byte{42}, make([]byte, 14)...),
		},
		{
			name:       "Zero value",
			buffer:     make([]byte, 15),
			index:      0,
			value:      0,
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: append([]byte{0}, make([]byte, 14)...),
		},
		{
			name:       "Value requiring two bytes",
			buffer:     make([]byte, 15),
			index:      0,
			value:      128, // 0x80, requires two bytes: 0x80, 0x01
			wantIdx:    2,
			wantErr:    false,
			wantBuffer: append([]byte{0x80, 0x01}, make([]byte, 13)...),
		},
		{
			name:       "Value requiring multiple bytes",
			buffer:     make([]byte, 15),
			index:      0,
			value:      16383, // 0x3FFF, requires two bytes: 0xFF, 0x7F
			wantIdx:    2,
			wantErr:    false,
			wantBuffer: append([]byte{0xFF, 0x7F}, make([]byte, 13)...),
		},
		{
			name:       "Large value requiring many bytes",
			buffer:     make([]byte, 15),
			index:      0,
			value:      0x1FFFFFFFFFFFFF, // Large value requiring multiple bytes
			wantIdx:    8,
			wantErr:    false,
			wantBuffer: append([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F}, make([]byte, 7)...),
		},
		{
			name:       "Serialization from middle index",
			buffer:     make([]byte, 15),
			index:      3,
			value:      127, // Single byte
			wantIdx:    4,
			wantErr:    false,
			wantBuffer: append([]byte{0x00, 0x00, 0x00, 127}, make([]byte, 11)...),
		},
		{
			name:    "Negative index",
			buffer:  make([]byte, 15),
			index:   -1,
			value:   100,
			wantIdx: -1,
			wantErr: true,
		},
		{
			name:    "Buffer too small",
			buffer:  make([]byte, 5), // Less than binary.MaxVarintLen64 (10)
			index:   0,
			value:   100,
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index near buffer end",
			buffer:  make([]byte, 15),
			index:   6, // 15 - 6 = 9, which is less than binary.MaxVarintLen64 (10)
			value:   100,
			wantIdx: 6,
			wantErr: true,
		},
		{
			name:    "Index at buffer boundary",
			buffer:  make([]byte, 15),
			index:   15,
			value:   100,
			wantIdx: 15,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the original buffer to compare
			originalBuffer := make([]byte, len(tt.buffer))
			copy(originalBuffer, tt.buffer)

			gotIdx, err := SerializeUnsignedVarInt(tt.buffer, tt.index, tt.value)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if gotIdx != tt.wantIdx {
				t.Errorf("SerializeUvarInt() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}

			if tt.wantBuffer != nil && !bytes.Equal(tt.buffer, tt.wantBuffer) {
				t.Errorf("SerializeUvarInt() buffer = %v, want %v", tt.buffer, tt.wantBuffer)
			}

			// Verify that the varint can be decoded back to the original value
			if !tt.wantErr && tt.wantBuffer != nil {
				decodedValue, bytesRead := binary.Uvarint(tt.buffer[tt.index:])
				if decodedValue != tt.value {
					t.Errorf("SerializeUvarInt() decoded value = %v, want %v", decodedValue, tt.value)
				}
				if tt.index+bytesRead != gotIdx {
					t.Errorf("SerializeUvarInt() bytes written = %v, want %v", bytesRead, gotIdx-tt.index)
				}
			}
		})
	}
}

func TestSerializeUnsignedVarInt_MaxValue(t *testing.T) {
	buffer := make([]byte, 15)
	value := uint64(18446744073709551615) // Max uint64

	gotIdx, err := SerializeUnsignedVarInt(buffer, 0, value)

	if err != nil {
		t.Errorf("unexpected error for max uint64: %v", err)
		return
	}

	// Verify decoding
	decodedValue, bytesRead := binary.Uvarint(buffer)
	if decodedValue != value {
		t.Errorf("decoded value = %v, want %v", decodedValue, value)
	}

	if bytesRead != gotIdx {
		t.Errorf("bytes written = %v, want %v", bytesRead, gotIdx)
	}

	// Max uint64 should use exactly 10 bytes in varint encoding
	if gotIdx != 10 {
		t.Errorf("max uint64 varint length = %v, want 10", gotIdx)
	}
}
