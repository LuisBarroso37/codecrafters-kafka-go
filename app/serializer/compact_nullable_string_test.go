package serializer

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestSerializeCompactNullableString(t *testing.T) {
	tests := []struct {
		name       string
		buffer     []byte
		index      int
		value      *string
		wantIdx    int
		wantErr    bool
		wantBuffer []byte
	}{
		{
			name:       "Null string",
			buffer:     make([]byte, 5),
			index:      0,
			value:      nil,
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Empty string",
			buffer:     make([]byte, 5),
			index:      0,
			value:      stringPtr(""),
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: []byte{0x01, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Single character string",
			buffer:     make([]byte, 5),
			index:      0,
			value:      stringPtr("a"),
			wantIdx:    2,
			wantErr:    false,
			wantBuffer: []byte{0x02, 'a', 0x00, 0x00, 0x00},
		},
		{
			name:       "Short string",
			buffer:     make([]byte, 10),
			index:      0,
			value:      stringPtr("test"),
			wantIdx:    5,
			wantErr:    false,
			wantBuffer: []byte{0x05, 't', 'e', 's', 't', 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "String from starting index",
			buffer:     make([]byte, 10),
			index:      3,
			value:      stringPtr("hi"),
			wantIdx:    6,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x00, 0x03, 'h', 'i', 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Null string from starting index",
			buffer:     make([]byte, 5),
			index:      2,
			value:      nil,
			wantIdx:    3,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:    "Negative index",
			buffer:  make([]byte, 5),
			index:   -1,
			value:   stringPtr("test"),
			wantIdx: -1,
			wantErr: true,
		},
		{
			name:    "Buffer too small for null string",
			buffer:  make([]byte, 0),
			index:   0,
			value:   nil,
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Buffer too small for string content",
			buffer:  make([]byte, 3),
			index:   0,
			value:   stringPtr("test"),
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Buffer too small for varint",
			buffer:  make([]byte, 1),
			index:   0,
			value:   stringPtr("a"),
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index beyond buffer",
			buffer:  make([]byte, 5),
			index:   5,
			value:   stringPtr("a"),
			wantIdx: 5,
			wantErr: true,
		},
		{
			name:    "Index at buffer boundary for null",
			buffer:  make([]byte, 5),
			index:   5,
			value:   nil,
			wantIdx: 5,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the original buffer to compare
			originalBuffer := make([]byte, len(tt.buffer))
			copy(originalBuffer, tt.buffer)

			gotIdx, err := SerializeCompactNullableString(tt.buffer, tt.index, tt.value)

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
				t.Errorf("SerializeCompactNullableString() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}

			if tt.wantBuffer != nil && !bytes.Equal(tt.buffer, tt.wantBuffer) {
				t.Errorf("SerializeCompactNullableString() buffer = %v, want %v", tt.buffer, tt.wantBuffer)
			}
		})
	}
}

func TestSerializeCompactNullableString_LargeVarint(t *testing.T) {
	// Test with a string that requires multi-byte varint encoding
	// String length 127 requires varint length 128 (which is 0x80, 0x01)
	longString := make([]byte, 127)
	for i := range longString {
		longString[i] = 'x'
	}
	longStringPtr := string(longString)

	// Calculate required buffer size: varint(128) + string content
	varintBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), 128)
	requiredSize := varintBytes + 127

	buffer := make([]byte, requiredSize)
	gotIdx, err := SerializeCompactNullableString(buffer, 0, &longStringPtr)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	expectedIdx := varintBytes + 127
	if gotIdx != expectedIdx {
		t.Errorf("SerializeCompactNullableString() gotIdx = %v, want %v", gotIdx, expectedIdx)
	}

	// Verify the varint was written correctly
	actualVarintValue, actualVarintBytes := binary.Uvarint(buffer)
	if actualVarintValue != 128 {
		t.Errorf("varint value = %v, want 128", actualVarintValue)
	}
	if actualVarintBytes != varintBytes {
		t.Errorf("varint bytes = %v, want %v", actualVarintBytes, varintBytes)
	}

	// Verify the string content
	stringContent := buffer[varintBytes : varintBytes+127]
	if !bytes.Equal(stringContent, longString) {
		t.Errorf("string content mismatch")
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
