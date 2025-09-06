package parser

import (
	"testing"
)

func TestExtractInt8(t *testing.T) {
	tests := []struct {
		name    string
		buffer  []byte
		index   int
		want    int8
		wantIdx int
		wantErr bool
	}{
		{
			name:    "Valid int8 positive",
			buffer:  []byte{0x01, 0x23, 0x45},
			index:   0,
			want:    0x01,
			wantIdx: 1,
			wantErr: false,
		},
		{
			name:    "Valid int8 negative",
			buffer:  []byte{0xFF, 0x23, 0x45},
			index:   0,
			want:    -1,
			wantIdx: 1,
			wantErr: false,
		},
		{
			name:    "Valid int8 from starting index",
			buffer:  []byte{0x00, 0x7F, 0x45},
			index:   1,
			want:    0x7F,
			wantIdx: 2,
			wantErr: false,
		},
		{
			name:    "Buffer too small",
			buffer:  []byte{},
			index:   0,
			want:    0,
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index beyond buffer",
			buffer:  []byte{0x01, 0x23},
			index:   5,
			want:    0,
			wantIdx: 5,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotIdx, err := ExtractInt8(tt.buffer, tt.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractInt8() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ExtractInt8() got = %v, want %v", got, tt.want)
			}
			if gotIdx != tt.wantIdx {
				t.Errorf("ExtractInt8() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}
		})
	}
}

func TestExtractInt16(t *testing.T) {
	tests := []struct {
		name    string
		buffer  []byte
		index   int
		want    int16
		wantIdx int
		wantErr bool
	}{
		{
			name:    "Valid int16",
			buffer:  []byte{0x01, 0x23, 0x45, 0x67},
			index:   0,
			want:    0x0123,
			wantIdx: 2,
			wantErr: false,
		},
		{
			name:    "Valid int16 from starting index",
			buffer:  []byte{0x00, 0x01, 0x23, 0x45},
			index:   1,
			want:    0x0123,
			wantIdx: 3,
			wantErr: false,
		},
		{
			name:    "Buffer too small",
			buffer:  []byte{0x01},
			index:   0,
			want:    0,
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index beyond buffer",
			buffer:  []byte{0x01, 0x23},
			index:   3,
			want:    0,
			wantIdx: 3,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotIdx, err := ExtractInt16(tt.buffer, tt.index)

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

			if got != tt.want {
				t.Errorf("extractInt16() got = %v, want %v", got, tt.want)
			}

			if gotIdx != tt.wantIdx {
				t.Errorf("extractInt16() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}
		})
	}
}

func TestExtractInt32(t *testing.T) {
	tests := []struct {
		name    string
		buffer  []byte
		index   int
		want    int32
		wantIdx int
		wantErr bool
	}{
		{
			name:    "Valid int32",
			buffer:  []byte{0x01, 0x23, 0x45, 0x67, 0x89},
			index:   0,
			want:    0x01234567,
			wantIdx: 4,
			wantErr: false,
		},
		{
			name:    "Valid int32 from starting index",
			buffer:  []byte{0x00, 0x01, 0x23, 0x45, 0x67},
			index:   1,
			want:    0x01234567,
			wantIdx: 5,
			wantErr: false,
		},
		{
			name:    "Buffer too small",
			buffer:  []byte{0x01, 0x23},
			index:   0,
			want:    0,
			wantIdx: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotIdx, err := ExtractInt32(tt.buffer, tt.index)

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

			if got != tt.want {
				t.Errorf("extractInt32() got = %v, want %v", got, tt.want)
			}

			if gotIdx != tt.wantIdx {
				t.Errorf("extractInt32() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}
		})
	}
}

func TestExtractNullableString(t *testing.T) {
	tests := []struct {
		name    string
		buffer  []byte
		index   int
		want    string
		wantIdx int
		wantErr bool
	}{
		{
			name:    "Valid string",
			buffer:  []byte{0x00, 0x04, 't', 'e', 's', 't'},
			index:   0,
			want:    "test",
			wantIdx: 6,
			wantErr: false,
		},
		{
			name:    "Null string (-1 length)",
			buffer:  []byte{0xFF, 0xFF},
			index:   0,
			want:    "",
			wantIdx: 2,
			wantErr: false,
		},
		{
			name:    "Empty string (0 length)",
			buffer:  []byte{0x00, 0x00},
			index:   0,
			want:    "",
			wantIdx: 2,
			wantErr: false,
		},
		{
			name:    "Buffer too small for length",
			buffer:  []byte{0x00},
			index:   0,
			want:    "",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Buffer too small for string content",
			buffer:  []byte{0x00, 0x05, 't', 'e', 's'},
			index:   0,
			want:    "",
			wantIdx: 2,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotIdx, err := ExtractNullableString(tt.buffer, tt.index)

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

			if got != tt.want {
				t.Errorf("extractNullableString() got = %q, want %q", got, tt.want)
			}

			if gotIdx != tt.wantIdx {
				t.Errorf("extractNullableString() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}
		})
	}
}

func TestExtractUnsignedVarInt(t *testing.T) {
	tests := []struct {
		name    string
		buffer  []byte
		index   int
		want    uint64
		wantIdx int
		wantErr bool
	}{
		{
			name:    "Single byte value (0)",
			buffer:  []byte{0x00},
			index:   0,
			want:    0,
			wantIdx: 1,
			wantErr: false,
		},
		{
			name:    "Single byte value (127)",
			buffer:  []byte{0x7F},
			index:   0,
			want:    127,
			wantIdx: 1,
			wantErr: false,
		},
		{
			name:    "Two byte value (128)",
			buffer:  []byte{0x80, 0x01},
			index:   0,
			want:    128,
			wantIdx: 2,
			wantErr: false,
		},
		{
			name:    "Two byte value (255)",
			buffer:  []byte{0xFF, 0x01},
			index:   0,
			want:    255,
			wantIdx: 2,
			wantErr: false,
		},
		{
			name:    "Three byte value (16384)",
			buffer:  []byte{0x80, 0x80, 0x01},
			index:   0,
			want:    16384,
			wantIdx: 3,
			wantErr: false,
		},
		{
			name:    "Value is read from starting index",
			buffer:  []byte{0xFF, 0x7F, 0x00},
			index:   1,
			want:    127,
			wantIdx: 2,
			wantErr: false,
		},
		{
			name:    "Buffer too small",
			buffer:  []byte{},
			index:   0,
			want:    0,
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Incomplete varint",
			buffer:  []byte{0x80},
			index:   0,
			want:    0,
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index beyond buffer",
			buffer:  []byte{0x7F},
			index:   1,
			want:    0,
			wantIdx: 1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotIdx, err := ExtractUnsignedVarInt(tt.buffer, tt.index)

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

			if got != tt.want {
				t.Errorf("ExtractUnsignedVarInt() got = %v, want %v", got, tt.want)
			}

			if gotIdx != tt.wantIdx {
				t.Errorf("ExtractUnsignedVarInt() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}
		})
	}
}

func TestExtractCompactString(t *testing.T) {
	tests := []struct {
		name    string
		buffer  []byte
		index   int
		want    string
		wantIdx int
		wantErr bool
	}{
		{
			name:    "Empty string (length 1)",
			buffer:  []byte{0x01},
			index:   0,
			want:    "",
			wantIdx: 1,
			wantErr: false,
		},
		{
			name:    "Valid string 'test' (length 5)",
			buffer:  []byte{0x05, 't', 'e', 's', 't'},
			index:   0,
			want:    "test",
			wantIdx: 5,
			wantErr: false,
		},
		{
			name:    "Single character 'a' (length 2)",
			buffer:  []byte{0x02, 'a'},
			index:   0,
			want:    "a",
			wantIdx: 2,
			wantErr: false,
		},
		{
			name:    "Longer string",
			buffer:  []byte{0x06, 'h', 'e', 'l', 'l', 'o'},
			index:   0,
			want:    "hello",
			wantIdx: 6,
			wantErr: false,
		},
		{
			name:    "String is read from starting index",
			buffer:  []byte{0xFF, 0x03, 'h', 'i'},
			index:   1,
			want:    "hi",
			wantIdx: 4,
			wantErr: false,
		},
		{
			name:    "Invalid length 0",
			buffer:  []byte{0x00},
			index:   0,
			want:    "",
			wantIdx: 1,
			wantErr: true,
		},
		{
			name:    "Buffer too small for varint",
			buffer:  []byte{},
			index:   0,
			want:    "",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Buffer too small for string content",
			buffer:  []byte{0x05, 't', 'e'},
			index:   0,
			want:    "",
			wantIdx: 1,
			wantErr: true,
		},
		{
			name:    "Incomplete varint",
			buffer:  []byte{0x80},
			index:   0,
			want:    "",
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Index beyond buffer",
			buffer:  []byte{0x01},
			index:   1,
			want:    "",
			wantIdx: 1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotIdx, err := ExtractCompactString(tt.buffer, tt.index)

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

			if got != tt.want {
				t.Errorf("ExtractCompactString() got = %q, want %q", got, tt.want)
			}

			if gotIdx != tt.wantIdx {
				t.Errorf("ExtractCompactString() gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}
		})
	}
}
