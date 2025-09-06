package serializer

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestSerializeTaggedFields(t *testing.T) {
	tests := []struct {
		name         string
		buffer       []byte
		index        int
		taggedFields map[string]string
		wantIdx      int
		wantErr      bool
		wantBuffer   []byte
	}{
		{
			name:         "Empty tagged fields",
			buffer:       make([]byte, 5),
			index:        0,
			taggedFields: map[string]string{},
			wantIdx:      1,
			wantErr:      false,
			wantBuffer:   []byte{0x00, 0x00, 0x00, 0x00, 0x00}, // just the length varint 0
		},
		{
			name:   "Single tagged field",
			buffer: make([]byte, 10),
			index:  0,
			taggedFields: map[string]string{
				"5": "test",
			},
			wantIdx:    7,
			wantErr:    false,
			wantBuffer: []byte{0x01, 0x05, 0x05, 't', 'e', 's', 't', 0x00, 0x00, 0x00},
		},
		{
			name:   "Multiple tagged fields",
			buffer: make([]byte, 15),
			index:  0,
			taggedFields: map[string]string{
				"1": "hi",
				"3": "bye",
			},
			wantIdx: 10,
			wantErr: false,
			// Note: map iteration order is not guaranteed, so we'll check differently
		},
		{
			name:   "Tagged field with empty value",
			buffer: make([]byte, 5),
			index:  0,
			taggedFields: map[string]string{
				"2": "",
			},
			wantIdx:    3,
			wantErr:    false,
			wantBuffer: []byte{0x01, 0x02, 0x01, 0x00, 0x00}, // length=1, tag=2, value=""
		},
		{
			name:   "Tagged fields from starting index",
			buffer: make([]byte, 10),
			index:  2,
			taggedFields: map[string]string{
				"0": "a",
			},
			wantIdx:    6,
			wantErr:    false,
			wantBuffer: []byte{0x00, 0x00, 0x01, 0x00, 0x02, 'a', 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:   "Large tag ID",
			buffer: make([]byte, 10),
			index:  0,
			taggedFields: map[string]string{
				"128": "x",
			},
			wantIdx:    5,
			wantErr:    false,
			wantBuffer: []byte{0x01, 0x80, 0x01, 0x02, 'x', 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:   "Invalid tag ID - non-numeric",
			buffer: make([]byte, 10),
			index:  0,
			taggedFields: map[string]string{
				"abc": "test",
			},
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:   "Invalid tag ID - negative",
			buffer: make([]byte, 10),
			index:  0,
			taggedFields: map[string]string{
				"-1": "test",
			},
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:   "Invalid tag ID - too large",
			buffer: make([]byte, 10),
			index:  0,
			taggedFields: map[string]string{
				"4294967296": "test", // 2^32
			},
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:   "Buffer too small for length",
			buffer: make([]byte, 0),
			index:  0,
			taggedFields: map[string]string{
				"1": "test",
			},
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:   "Buffer too small for tag",
			buffer: make([]byte, 2),
			index:  0,
			taggedFields: map[string]string{
				"1": "test",
			},
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:   "Buffer too small for value",
			buffer: make([]byte, 3),
			index:  0,
			taggedFields: map[string]string{
				"1": "test",
			},
			wantIdx: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the original buffer for comparison
			originalBuffer := make([]byte, len(tt.buffer))
			copy(originalBuffer, tt.buffer)

			gotIdx, err := SerializeTaggedFields(tt.buffer, tt.index, tt.taggedFields)

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
				t.Errorf("SerializeTaggedFields() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}

			// For single field tests or empty fields, we can check exact buffer match
			if tt.wantBuffer != nil && (len(tt.taggedFields) <= 1) {
				if !bytes.Equal(tt.buffer, tt.wantBuffer) {
					t.Errorf("SerializeTaggedFields() buffer = %v, want %v", tt.buffer, tt.wantBuffer)
				}
			}

			// For multiple fields, verify the structure separately due to map iteration order
			if len(tt.taggedFields) > 1 && !tt.wantErr {
				// Verify the length field
				actualLength, lengthBytes := binary.Uvarint(tt.buffer[tt.index:])
				if actualLength != uint64(len(tt.taggedFields)) {
					t.Errorf("SerializeTaggedFields() length = %v, want %v", actualLength, len(tt.taggedFields))
				}

				// Parse and verify each field exists
				idx := tt.index + lengthBytes
				fieldsFound := make(map[string]string)

				for i := 0; i < int(actualLength); i++ {
					// Read tag ID
					tagID, tagBytes := binary.Uvarint(tt.buffer[idx:])
					idx += tagBytes

					// Read value length
					valueLength, valueLengthBytes := binary.Uvarint(tt.buffer[idx:])
					idx += valueLengthBytes

					// Read value content
					var value string
					if valueLength > 1 {
						value = string(tt.buffer[idx : idx+int(valueLength-1)])
						idx += int(valueLength - 1)
					}

					fieldsFound[string(rune(tagID+'0'))] = value
				}

				// Compare found fields with expected
				for expectedTag, expectedValue := range tt.taggedFields {
					if actualValue, exists := fieldsFound[expectedTag]; !exists || actualValue != expectedValue {
						t.Errorf("SerializeTaggedFields() missing or incorrect field %s: got %q, want %q", expectedTag, actualValue, expectedValue)
					}
				}
			}
		})
	}
}

func TestSerializeTaggedFields_LargeFields(t *testing.T) {
	// Test with many fields
	taggedFields := make(map[string]string)
	for i := 0; i < 10; i++ {
		taggedFields[string(rune('0'+i))] = "value" + string(rune('0'+i))
	}

	// Calculate required buffer size
	requiredSize := 1 // length varint
	for _, value := range taggedFields {
		requiredSize += 1          // tag varint (single digit)
		requiredSize += 1          // value length varint
		requiredSize += len(value) // value content
	}

	buffer := make([]byte, requiredSize)
	gotIdx, err := SerializeTaggedFields(buffer, 0, taggedFields)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	if gotIdx != requiredSize {
		t.Errorf("SerializeTaggedFields() gotIdx = %v, want %v", gotIdx, requiredSize)
	}

	// Verify the length field
	actualLength, _ := binary.Uvarint(buffer)
	if actualLength != uint64(len(taggedFields)) {
		t.Errorf("SerializeTaggedFields() length = %v, want %v", actualLength, len(taggedFields))
	}
}

func TestSerializeTaggedFields_EdgeCaseTagIDs(t *testing.T) {
	tests := []struct {
		name  string
		tagID string
		valid bool
	}{
		{"Zero tag ID", "0", true},
		{"Max uint32", "4294967295", false}, // Actually too large for our buffer
		{"Floating point", "1.5", false},
		{"Empty string", "", false},
		{"Hexadecimal", "0xFF", false},
		{"Leading zeros", "007", true},
		{"Scientific notation", "1e3", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := make([]byte, 10)
			taggedFields := map[string]string{
				tt.tagID: "test",
			}

			_, err := SerializeTaggedFields(buffer, 0, taggedFields)

			if tt.valid && err != nil {
				t.Errorf("expected valid tag ID %q but got error: %v", tt.tagID, err)
			}

			if !tt.valid && err == nil {
				t.Errorf("expected invalid tag ID %q to produce error but got none", tt.tagID)
			}
		})
	}
}
