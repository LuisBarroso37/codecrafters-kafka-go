package request

import (
	"testing"
)

func TestParseRequestHeader(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		want      RequestHeader
		wantIndex int
		wantErr   bool
	}{
		{
			name: "ApiVersions request with body and tagged fields (flexible version)",
			input: []byte{
				0x00, 0x00, 0x00, 0x18, // MessageSize: 24
				0x00, 0x12, // RequestApiKey: 18 (ApiVersions)
				0x00, 0x04, // RequestApiVersion: 4 (flexible)
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
				0x01,          // Number of tagged fields (varint, 1)
				0x00,          // Tag ID (varint, 0)
				0x04,          // Value length (varint, 4 = "bar" length 3 + 1)
				'b', 'a', 'r', // Value: "bar"
			},
			want: RequestHeader{
				MessageSize:       24,
				RequestApiKey:     18,
				RequestApiVersion: 4,
				CorrelationId:     66,
				ClientId:          "test",
				TaggedFields:      map[string]string{"0": "bar"},
			},
			wantIndex: 24,
			wantErr:   false,
		},
		{
			name: "ApiVersions request with body and no tagged fields (flexible version)",
			input: []byte{
				0x00, 0x00, 0x00, 0x13, // MessageSize: 19
				0x00, 0x12, // RequestApiKey: 18 (ApiVersions)
				0x00, 0x03, // RequestApiVersion: 3 (flexible)
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
				0x00, // Number of tagged fields (varint, 0)
			},
			want: RequestHeader{
				MessageSize:       19,
				RequestApiKey:     18,
				RequestApiVersion: 3,
				CorrelationId:     66,
				ClientId:          "test",
				TaggedFields:      map[string]string{},
			},
			wantIndex: 19,
			wantErr:   false,
		},
		{
			name: "ApiVersions request with body (non-flexible version, no tagged fields)",
			input: []byte{
				0x00, 0x00, 0x00, 0x15, // MessageSize: 21
				0x00, 0x12, // RequestApiKey: 18 (ApiVersions)
				0x00, 0x02, // RequestApiVersion: 2 (non-flexible)
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
			},
			want: RequestHeader{
				MessageSize:       21,
				RequestApiKey:     18,
				RequestApiVersion: 2,
				CorrelationId:     66,
				ClientId:          "test",
				TaggedFields:      map[string]string{},
			},
			wantIndex: 18,
			wantErr:   false,
		},
		{
			name:      "Empty buffer",
			input:     []byte{},
			want:      RequestHeader{},
			wantIndex: 0,
			wantErr:   true,
		},
		{
			name:      "Buffer too small for message size",
			input:     []byte{0x00, 0x00},
			want:      RequestHeader{},
			wantIndex: 0,
			wantErr:   true,
		},
		{
			name: "Buffer too small for complete header",
			input: []byte{
				0x00, 0x00, 0x00, 0x10, // MessageSize: 16
				0x00, 0x12, // RequestApiKey: 18
				// Missing rest of header
			},
			want:      RequestHeader{},
			wantIndex: 0,
			wantErr:   true,
		},
		{
			name: "Null ClientId",
			input: []byte{
				0x00, 0x00, 0x00, 0x0C, // MessageSize: 12
				0x00, 0x12, // RequestApiKey: 18 (ApiVersions)
				0x00, 0x02, // RequestApiVersion: 2 (non-flexible)
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0xFF, 0xFF, // ClientId length: -1 (null)
			},
			want: RequestHeader{
				MessageSize:       12,
				RequestApiKey:     18,
				RequestApiVersion: 2,
				CorrelationId:     66,
				ClientId:          "",
				TaggedFields:      map[string]string{},
			},
			wantIndex: 14,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, index, err := ParseRequestHeader(tt.input, 0)

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

			if index != tt.wantIndex {
				t.Errorf("index mismatch: got %d, want %d", index, tt.wantIndex)
			}

			if got.MessageSize != tt.want.MessageSize ||
				got.RequestApiKey != tt.want.RequestApiKey ||
				got.RequestApiVersion != tt.want.RequestApiVersion ||
				got.CorrelationId != tt.want.CorrelationId ||
				got.ClientId != tt.want.ClientId {
				t.Errorf("got = %+v, want %+v", got, tt.want)
			}
		})
	}
}
