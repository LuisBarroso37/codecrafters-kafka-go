package parsing

import (
	"testing"
)

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
			name:    "Valid int16 at offset",
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
			name:    "Valid int32 at offset",
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
