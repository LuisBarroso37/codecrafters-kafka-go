package serializer

import (
	"bytes"
	"testing"
)

func TestSerializeBoolean(t *testing.T) {
	tests := []struct {
		name    string
		buffer  []byte
		index   int
		value   bool
		want    byte
		wantIdx int
		wantErr bool
	}{
		{
			name:    "Valid boolean - true",
			buffer:  make([]byte, 1),
			index:   0,
			value:   true,
			want:    0x01,
			wantIdx: 1,
			wantErr: false,
		},
		{
			name:    "Valid boolean - false",
			buffer:  make([]byte, 1),
			index:   0,
			value:   false,
			want:    0x00,
			wantIdx: 1,
			wantErr: false,
		},
		{
			name:    "Valid boolean from starting index",
			buffer:  make([]byte, 2),
			index:   1,
			value:   true,
			want:    0x01,
			wantIdx: 2,
			wantErr: false,
		},
		{
			name:    "Index beyond buffer",
			buffer:  make([]byte, 1),
			index:   1,
			value:   true,
			want:    0x00,
			wantIdx: 0,
			wantErr: true,
		},
		{
			name:    "Negative index",
			buffer:  make([]byte, 1),
			index:   -1,
			value:   true,
			want:    0x00,
			wantIdx: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIdx, err := SerializeBoolean(tt.buffer, tt.index, tt.value)
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
				t.Errorf("gotIdx = %v, want %v", gotIdx, tt.wantIdx)
			}

			if !bytes.Equal(tt.buffer[tt.index:tt.wantIdx], []byte{tt.want}) {
				t.Errorf("got = %v, want %v", tt.buffer[tt.index], tt.want)
			}
		})
	}
}
