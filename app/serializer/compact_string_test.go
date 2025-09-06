package serializer

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestSerializeCompactString(t *testing.T) {
	tests := []struct {
		name       string
		buffer     []byte
		index      int
		value      string
		wantIdx    int
		wantErr    bool
		wantBuffer []byte
	}{
		{
			name:       "Empty string",
			buffer:     make([]byte, 5),
			index:      0,
			value:      "",
			wantIdx:    1,
			wantErr:    false,
			wantBuffer: []byte{0x01, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Single character string",
			buffer:     make([]byte, 5),
			index:      0,
			value:      "a",
			wantIdx:    2,
			wantErr:    false,
			wantBuffer: []byte{0x02, 'a', 0x00, 0x00, 0x00},
		},
		{
			name:       "Short string",
			buffer:     make([]byte, 10),
			index:      0,
			value:      "test",
			wantIdx:    5,
			wantErr:    false,
			wantBuffer: []byte{0x05, 't', 'e', 's', 't', 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "String from starting index",
			buffer:     make([]byte, 10),
			index:      3,
			value:      "hi",
			wantIdx:    6,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x00, 0x03, 'h', 'i', 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:       "Longer string",
			buffer:     make([]byte, 15),
			index:      0,
			value:      "hello world",
			wantIdx:    12,
			wantErr:    false,
			wantBuffer: []byte{0x0C, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', 0x00, 0x00, 0x00},
		},
		{
			name:       "String with special characters",
			buffer:     make([]byte, 10),
			index:      0,
			value:      "café",
			wantIdx:    6,
			wantErr:    false,
			wantBuffer: []byte{0x06, 'c', 'a', 'f', 0xc3, 0xa9, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:    "Buffer too small for string content",
			buffer:  make([]byte, 3),
			index:   0,
			value:   "test",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Buffer too small for varint",
			buffer:  make([]byte, 1),
			index:   0,
			value:   "a",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index beyond buffer",
			buffer:  make([]byte, 5),
			index:   5,
			value:   "a",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index at buffer boundary",
			buffer:  make([]byte, 5),
			index:   4,
			value:   "a",
			wantIdx: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the original buffer for comparison
			originalBuffer := make([]byte, len(tt.buffer))
			copy(originalBuffer, tt.buffer)

			gotIdx, err := SerializeCompactString(tt.buffer, tt.index, tt.value)

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
				t.Errorf("SerializeCompactString() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}

			if tt.wantBuffer != nil && !bytes.Equal(tt.buffer, tt.wantBuffer) {
				t.Errorf("SerializeCompactString() buffer = %v, want %v", tt.buffer, tt.wantBuffer)
			}

			// Verify that only the intended bytes were modified
			if !tt.wantErr && tt.wantBuffer != nil {
				// Check bytes before the string
				for i := 0; i < tt.index; i++ {
					if tt.buffer[i] != originalBuffer[i] {
						t.Errorf("SerializeCompactString() unexpectedly modified buffer[%d] = 0x%02X, original was 0x%02X",
							i, tt.buffer[i], originalBuffer[i])
					}
				}

				// Check bytes after the string
				for i := gotIdx; i < len(tt.buffer); i++ {
					if tt.buffer[i] != originalBuffer[i] {
						t.Errorf("SerializeCompactString() unexpectedly modified buffer[%d] = 0x%02X, original was 0x%02X",
							i, tt.buffer[i], originalBuffer[i])
					}
				}
			}
		})
	}
}

func TestSerializeCompactString_LargeVarint(t *testing.T) {
	// Test with a string that requires multi-byte varint encoding
	// String length 127 requires varint length 128 (which is 0x80, 0x01)
	longString := strings.Repeat("x", 127)

	// Calculate required buffer size: varint(128) + string content
	varintBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), 128)
	requiredSize := varintBytes + 127

	buffer := make([]byte, requiredSize)
	gotIdx, err := SerializeCompactString(buffer, 0, longString)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	expectedIdx := varintBytes + 127
	if gotIdx != expectedIdx {
		t.Errorf("SerializeCompactString() gotIdx = %v, want %v", gotIdx, expectedIdx)
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
	expectedContent := []byte(longString)
	if !bytes.Equal(stringContent, expectedContent) {
		t.Errorf("string content mismatch")
	}
}

func TestSerializeCompactString_VeryLargeVarint(t *testing.T) {
	// Test with a string that requires even larger varint encoding
	// String length 16383 requires varint length 16384 (which is 0x80, 0x80, 0x01)
	longString := strings.Repeat("y", 16383)

	// Calculate required buffer size: varint(16384) + string content
	varintBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), 16384)
	requiredSize := varintBytes + 16383

	buffer := make([]byte, requiredSize)
	gotIdx, err := SerializeCompactString(buffer, 0, longString)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	expectedIdx := varintBytes + 16383
	if gotIdx != expectedIdx {
		t.Errorf("SerializeCompactString() gotIdx = %v, want %v", gotIdx, expectedIdx)
	}

	// Verify the varint was written correctly
	actualVarintValue, actualVarintBytes := binary.Uvarint(buffer)
	if actualVarintValue != 16384 {
		t.Errorf("varint value = %v, want 16384", actualVarintValue)
	}
	if actualVarintBytes != varintBytes {
		t.Errorf("varint bytes = %v, want %v", actualVarintBytes, varintBytes)
	}
}

func TestSerializeCompactString_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantLen int // expected varint value (string length + 1)
	}{
		{
			name:    "Empty string produces varint 1",
			value:   "",
			wantLen: 1,
		},
		{
			name:    "Length 126 produces varint 127 (single byte)",
			value:   strings.Repeat("z", 126),
			wantLen: 127,
		},
		{
			name:    "Length 127 produces varint 128 (multi-byte)",
			value:   strings.Repeat("w", 127),
			wantLen: 128,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate buffer size needed
			varintBytes := binary.PutUvarint(make([]byte, binary.MaxVarintLen64), uint64(tt.wantLen))
			bufferSize := varintBytes + len(tt.value)
			buffer := make([]byte, bufferSize)

			gotIdx, err := SerializeCompactString(buffer, 0, tt.value)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Verify the varint
			actualVarintValue, _ := binary.Uvarint(buffer)
			if actualVarintValue != uint64(tt.wantLen) {
				t.Errorf("varint value = %v, want %v", actualVarintValue, tt.wantLen)
			}

			// Verify the index advancement
			expectedIdx := varintBytes + len(tt.value)
			if gotIdx != expectedIdx {
				t.Errorf("gotIdx = %v, want %v", gotIdx, expectedIdx)
			}
		})
	}
}
