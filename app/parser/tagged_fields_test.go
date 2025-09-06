package parser

import (
	"reflect"
	"testing"
)

func TestExtractTagFields(t *testing.T) {
	tests := []struct {
		name    string
		buffer  []byte
		index   int
		want    map[string]string
		wantIdx int
		wantErr bool
	}{
		{
			name:    "No tagged fields (length 0)",
			buffer:  []byte{0x00},
			index:   0,
			want:    map[string]string{},
			wantIdx: 1,
			wantErr: false,
		},
		{
			name:    "Single tagged field",
			buffer:  []byte{0x01, 0x05, 0x05, 't', 'e', 's', 't'},
			index:   0,
			want:    map[string]string{"5": "test"},
			wantIdx: 7,
			wantErr: false,
		},
		{
			name:    "Multiple tagged fields",
			buffer:  []byte{0x02, 0x01, 0x03, 'h', 'i', 0x03, 0x04, 'b', 'y', 'e'},
			index:   0,
			want:    map[string]string{"1": "hi", "3": "bye"},
			wantIdx: 10,
			wantErr: false,
		},
		{
			name:    "Tagged field with empty string",
			buffer:  []byte{0x01, 0x02, 0x01},
			index:   0,
			want:    map[string]string{"2": ""},
			wantIdx: 3,
			wantErr: false,
		},
		{
			name:    "Tagged fields from starting index",
			buffer:  []byte{0xFF, 0x01, 0x00, 0x02, 'a'},
			index:   1,
			want:    map[string]string{"0": "a"},
			wantIdx: 5,
			wantErr: false,
		},
		{
			name:    "Tagged field with multi-byte varint tag",
			buffer:  []byte{0x01, 0x80, 0x01, 0x02, 'x'},
			index:   0,
			want:    map[string]string{"128": "x"},
			wantIdx: 5,
			wantErr: false,
		},
		{
			name:    "Buffer too small for length",
			buffer:  []byte{},
			index:   0,
			want:    map[string]string{},
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Buffer too small for field tag",
			buffer:  []byte{0x01},
			index:   0,
			want:    map[string]string{},
			wantIdx: 1,
			wantErr: true,
		},
		{
			name:    "Buffer too small for field value",
			buffer:  []byte{0x01, 0x01, 0x05, 't', 'e'},
			index:   0,
			want:    nil,
			wantIdx: 3,
			wantErr: true,
		},
		{
			name:    "Incomplete varint in length",
			buffer:  []byte{0x80},
			index:   0,
			want:    map[string]string{},
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Incomplete varint in field tag",
			buffer:  []byte{0x01, 0x80},
			index:   0,
			want:    map[string]string{},
			wantIdx: 2,
			wantErr: true,
		},
		{
			name:    "Invalid compact string in field value",
			buffer:  []byte{0x01, 0x01, 0x00},
			index:   0,
			want:    nil,
			wantIdx: 3,
			wantErr: true,
		},
		{
			name:    "Index beyond buffer",
			buffer:  []byte{0x00},
			index:   1,
			want:    map[string]string{},
			wantIdx: 1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotIdx, err := ExtractTagFields(tt.buffer, tt.index)

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

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractTagFields() got = %v, want %v", got, tt.want)
			}

			if gotIdx != tt.wantIdx {
				t.Errorf("ExtractTagFields() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}
		})
	}
}
