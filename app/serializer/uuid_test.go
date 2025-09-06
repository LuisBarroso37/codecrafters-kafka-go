package serializer

import (
	"bytes"
	"testing"
)

func TestSerializeUUID(t *testing.T) {
	tests := []struct {
		name       string
		buffer     []byte
		index      int
		uuidStr    string
		wantIdx    int
		wantErr    bool
		wantBuffer []byte
	}{
		{
			name:    "Valid UUID with dashes",
			buffer:  make([]byte, 20),
			index:   0,
			uuidStr: "550e8400-e29b-41d4-a716-446655440000",
			wantIdx: 16,
			wantErr: false,
			wantBuffer: []byte{
				0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
				0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
		},
		{
			name:    "Valid UUID without dashes",
			buffer:  make([]byte, 20),
			index:   0,
			uuidStr: "550e8400e29b41d4a716446655440000",
			wantIdx: 16,
			wantErr: false,
			wantBuffer: []byte{
				0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
				0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
		},
		{
			name:    "UUID from starting index",
			buffer:  make([]byte, 20),
			index:   2,
			uuidStr: "00000000-0000-0000-0000-000000000001",
			wantIdx: 18,
			wantErr: false,
			wantBuffer: []byte{
				0x00, 0x00, // offset bytes
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				0x00, 0x00, // remaining bytes
			},
		},
		{
			name:       "All zeros UUID",
			buffer:     make([]byte, 16),
			index:      0,
			uuidStr:    "00000000-0000-0000-0000-000000000000",
			wantIdx:    16,
			wantErr:    false,
			wantBuffer: make([]byte, 16), // all zeros
		},
		{
			name:    "All ones UUID",
			buffer:  make([]byte, 16),
			index:   0,
			uuidStr: "ffffffff-ffff-ffff-ffff-ffffffffffff",
			wantIdx: 16,
			wantErr: false,
			wantBuffer: []byte{
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			},
		},
		{
			name:    "Mixed case UUID",
			buffer:  make([]byte, 16),
			index:   0,
			uuidStr: "550E8400-E29B-41D4-A716-446655440000",
			wantIdx: 16,
			wantErr: false,
			wantBuffer: []byte{
				0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
				0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
			},
		},
		{
			name:    "Buffer too small",
			buffer:  make([]byte, 10),
			index:   0,
			uuidStr: "550e8400-e29b-41d4-a716-446655440000",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Buffer too small from starting index",
			buffer:  make([]byte, 16),
			index:   5,
			uuidStr: "550e8400-e29b-41d4-a716-446655440000",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Invalid hex character",
			buffer:  make([]byte, 16),
			index:   0,
			uuidStr: "550g8400-e29b-41d4-a716-446655440000",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "UUID too short",
			buffer:  make([]byte, 16),
			index:   0,
			uuidStr: "550e8400-e29b-41d4-a716-4466554400",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "UUID too long",
			buffer:  make([]byte, 20),
			index:   0,
			uuidStr: "550e8400-e29b-41d4-a716-44665544000000",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Empty UUID string",
			buffer:  make([]byte, 16),
			index:   0,
			uuidStr: "",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "UUID with invalid format",
			buffer:  make([]byte, 16),
			index:   0,
			uuidStr: "not-a-uuid",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "UUID with extra dashes",
			buffer:  make([]byte, 16),
			index:   0,
			uuidStr: "550e-8400-e29b-41d4-a716-446655440000",
			wantIdx: 16,
			wantErr: false,
			wantBuffer: []byte{
				0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
				0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the original buffer for comparison
			originalBuffer := make([]byte, len(tt.buffer))
			copy(originalBuffer, tt.buffer)

			gotIdx, err := SerializeUUID(tt.buffer, tt.index, tt.uuidStr)

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
				t.Errorf("SerializeUUID() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}

			if tt.wantBuffer != nil && !bytes.Equal(tt.buffer, tt.wantBuffer) {
				t.Errorf("SerializeUUID() buffer = %v, want %v", tt.buffer, tt.wantBuffer)
			}

			// Verify that only the UUID bytes were modified
			if !tt.wantErr && tt.wantBuffer != nil {
				// Check bytes before the UUID
				for i := 0; i < tt.index; i++ {
					if tt.buffer[i] != originalBuffer[i] {
						t.Errorf("SerializeUUID() unexpectedly modified buffer[%d] = 0x%02X, original was 0x%02X",
							i, tt.buffer[i], originalBuffer[i])
					}
				}
				// Check bytes after the UUID
				for i := tt.index + 16; i < len(tt.buffer); i++ {
					if tt.buffer[i] != originalBuffer[i] {
						t.Errorf("SerializeUUID() unexpectedly modified buffer[%d] = 0x%02X, original was 0x%02X",
							i, tt.buffer[i], originalBuffer[i])
					}
				}
			}
		})
	}
}

func TestSerializeUUID_SpecificPatterns(t *testing.T) {
	// Test some well-known UUID patterns
	patterns := []struct {
		name     string
		uuid     string
		expected []byte
	}{
		{
			name: "Version 4 UUID example",
			uuid: "123e4567-e89b-12d3-a456-426614174000",
			expected: []byte{
				0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3,
				0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00,
			},
		},
		{
			name: "Nil UUID",
			uuid: "00000000-0000-0000-0000-000000000000",
			expected: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		},
		{
			name: "Max UUID",
			uuid: "ffffffff-ffff-ffff-ffff-ffffffffffff",
			expected: []byte{
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			},
		},
	}

	for _, pattern := range patterns {
		t.Run(pattern.name, func(t *testing.T) {
			buffer := make([]byte, 16)
			gotIdx, err := SerializeUUID(buffer, 0, pattern.uuid)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if gotIdx != 16 {
				t.Errorf("SerializeUUID() gotIdx = %v, want 16", gotIdx)
			}

			if !bytes.Equal(buffer, pattern.expected) {
				t.Errorf("SerializeUUID() buffer = %v, want %v", buffer, pattern.expected)
			}
		})
	}
}
