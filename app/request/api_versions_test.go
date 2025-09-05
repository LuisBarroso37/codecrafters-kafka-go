package request

import (
	"reflect"
	"testing"
)

func TestParseRequestBody(t *testing.T) {
	handler := ApiVersionsHandler{
		supportedApis: []ApiVersion{
			{ApiKey: 18, MinVersion: 0, MaxVersion: 4},
		},
	}
	baseHeader := RequestHeader{
		MessageSize:   17,
		RequestApiKey: 18,
		CorrelationId: 66,
		ClientId:      "test",
		TaggedFields:  map[string]string{},
	}

	tests := []struct {
		name      string
		version   int16
		input     []byte
		bodyIndex int
		want      ApiVersionsRequest
		wantErr   bool
	}{
		{
			name:    "ApiVersions request with body and tagged fields (flexible version)",
			version: 4,
			input: []byte{
				// Header
				0x00, 0x00, 0x00, 0x20, // MessageSize: 32
				0x00, 0x12, // RequestApiKey: 18 (ApiVersions)
				0x00, 0x04, // RequestApiVersion: 4 (flexible)
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
				0x00, // Number of header tagged fields (varint, 0)
				// Body starts here
				0x07, 'g', 'o', '-', 'c', 'l', 'i', // clientSoftwareName: "go-cli" (compact string: length 6+1=7)
				0x06, '1', '.', '2', '.', '3', // clientSoftwareVersion: "1.2.3" (compact string: length 5+1=6)
				0x01,                    // Number of tagged fields (varint, 1)
				0x02,                    // Tag ID (varint, 2)
				0x06,                    // Value length (varint, 5+1=6 for "value")
				'v', 'a', 'l', 'u', 'e', // Value: "value"
			},
			bodyIndex: 19,
			want: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       32,
					RequestApiKey:     18,
					RequestApiVersion: 4,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				ClientSoftwareName:    "go-cli",
				ClientSoftwareVersion: "1.2.3",
				TaggedFields:          map[string]string{"2": "value"},
			},
			wantErr: false,
		},
		{
			name:    "ApiVersions request with body and no tagged fields (flexible version)",
			version: 4,
			input: []byte{
				// Header
				0x00, 0x00, 0x00, 0x1C, // MessageSize: 28
				0x00, 0x12, // RequestApiKey: 18
				0x00, 0x04, // RequestApiVersion: 4
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
				0x00, // Number of header tagged fields (varint, 0)
				// Body
				0x07, 'g', 'o', '-', 'c', 'l', 'i', // clientSoftwareName: "go-cli"
				0x06, '1', '.', '2', '.', '3', // clientSoftwareVersion: "1.2.3"
				0x00, // Number of tagged fields (varint, 0)
			},
			bodyIndex: 19,
			want: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       28,
					RequestApiKey:     18,
					RequestApiVersion: 4,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				ClientSoftwareName:    "go-cli",
				ClientSoftwareVersion: "1.2.3",
				TaggedFields:          map[string]string{},
			},
			wantErr: false,
		},
		{
			name:    "ApiVersions request (non-flexible version, no client software fields)",
			version: 2,
			input: []byte{
				0x00, 0x00, 0x00, 0x0E, // MessageSize: 14
				0x00, 0x12, // RequestApiKey: 18
				0x00, 0x02, // RequestApiVersion: 2
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
				// No tagged fields in non-flexible versions
				// No body for ApiVersions v2
			},
			bodyIndex: 18, // Start after header (no tagged fields in v2)
			want: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       14,
					RequestApiKey:     18,
					RequestApiVersion: 2,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				ClientSoftwareName:    "",
				ClientSoftwareVersion: "",
				TaggedFields:          map[string]string{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := baseHeader
			header.RequestApiVersion = tt.version

			got, err := handler.ParseRequestBody(header, tt.input, tt.bodyIndex)

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

			apiReq, ok := got.(*ApiVersionsRequest)
			if !ok {
				t.Errorf("ApiVersionsHandler received %T instead of *ApiVersionsRequest", got)
				return
			}

			if apiReq.ClientSoftwareName != tt.want.ClientSoftwareName {
				t.Errorf("ClientSoftwareName mismatch: got %q, want %q", apiReq.ClientSoftwareName, tt.want.ClientSoftwareName)
			}

			if apiReq.ClientSoftwareVersion != tt.want.ClientSoftwareVersion {
				t.Errorf("ClientSoftwareVersion mismatch: got %q, want %q", apiReq.ClientSoftwareVersion, tt.want.ClientSoftwareVersion)
			}

			if apiReq.TaggedFields == nil {
				apiReq.TaggedFields = map[string]string{}
			}

			if tt.want.TaggedFields == nil {
				tt.want.TaggedFields = map[string]string{}
			}

			if len(apiReq.TaggedFields) != len(tt.want.TaggedFields) {
				t.Errorf("TaggedFields length mismatch: got %d, want %d", len(apiReq.TaggedFields), len(tt.want.TaggedFields))
			}

			for key, wantValue := range tt.want.TaggedFields {
				gotValue, exists := apiReq.TaggedFields[key]
				if !exists {
					t.Errorf("TaggedFields missing key %q", key)
				} else if gotValue != wantValue {
					t.Errorf("TaggedFields[%q] mismatch: got %q, want %q", key, gotValue, wantValue)
				}
			}

			for key := range apiReq.TaggedFields {
				if _, exists := tt.want.TaggedFields[key]; !exists {
					t.Errorf("TaggedFields has unexpected key %q", key)
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name  string
		input ApiVersionsRequest
		want  error
	}{
		{
			name: "Valid ApiVersions request version 0",
			input: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       16,
					RequestApiKey:     18,
					RequestApiVersion: 0,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      make(map[string]string),
				},
			},
			want: nil,
		},
		{
			name: "Valid ApiVersions request version 4",
			input: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       16,
					RequestApiKey:     18,
					RequestApiVersion: 4,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      make(map[string]string),
				},
				ClientSoftwareName:    "go-cli",
				ClientSoftwareVersion: "1.2.3",
				TaggedFields:          make(map[string]string),
			},
			want: nil,
		},
		{
			name: "Unsupported request API version",
			input: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       16,
					RequestApiKey:     18,
					RequestApiVersion: 5,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      make(map[string]string),
				},
			},
			want: &RequestParseError{Code: 35, Message: "Invalid version"},
		},
		{
			name: "Negative API version",
			input: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       16,
					RequestApiKey:     18,
					RequestApiVersion: -1,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      make(map[string]string),
				},
			},
			want: &RequestParseError{Code: 35, Message: "Invalid version"},
		},
		{
			name: "Invalid ApiVersions request version 2",
			input: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       16,
					RequestApiKey:     18,
					RequestApiVersion: 2,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      make(map[string]string),
				},
				ClientSoftwareName:    "go-cli",
				ClientSoftwareVersion: "1.2.3",
				TaggedFields:          make(map[string]string),
			},
			want: &RequestParseError{Code: 42, Message: "Client software name must not be set"},
		},
		{
			name: "Invalid ApiVersions request version 4",
			input: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       16,
					RequestApiKey:     18,
					RequestApiVersion: 4,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      make(map[string]string),
				},
				ClientSoftwareName:    "",
				ClientSoftwareVersion: "1.2.3",
				TaggedFields:          make(map[string]string),
			},
			want: &RequestParseError{Code: 42, Message: "Client software name is required"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.input.Validate()

			if err == nil && tt.want != nil {
				t.Errorf("expected error %v but got nil", tt.want)
			}
			if err != nil && tt.want == nil {
				t.Errorf("unexpected error: got %v, want nil", err)
			}

			if err != nil && tt.want != nil {
				gotErr, ok := err.(*RequestParseError)
				wantErr, ok2 := tt.want.(*RequestParseError)

				if !ok || !ok2 || gotErr.Code != wantErr.Code || gotErr.Message != wantErr.Message {
					t.Errorf("unexpected error: got %v, want %v", err, tt.want)
				}
			}
		})
	}
}

func TestHandleRequest(t *testing.T) {
	handler := ApiVersionsHandler{
		supportedApis: []ApiVersion{
			{ApiKey: 18, MinVersion: 0, MaxVersion: 4, TaggedFields: map[string]string{}},
		},
	}

	tests := []struct {
		name    string
		request ApiVersionsRequest
		want    ApiVersionsResponse
	}{
		{
			name: "Valid request - should return error code 0",
			request: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       16,
					RequestApiKey:     18,
					RequestApiVersion: 4,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				ClientSoftwareName:    "go-cli",
				ClientSoftwareVersion: "1.2.3",
				TaggedFields:          map[string]string{},
			},
			want: ApiVersionsResponse{
				CorrelationId: 66,
				ErrorCode:     0,
				ApiKeys: []ApiVersion{
					{ApiKey: 18, MinVersion: 0, MaxVersion: 4, TaggedFields: map[string]string{}},
				},
				ThrottleTime: 0,
				TaggedFields: map[string]string{},
			},
		},
		{
			name: "Valid request version 2 (non-flexible) - should return error code 0",
			request: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       16,
					RequestApiKey:     18,
					RequestApiVersion: 2,
					CorrelationId:     123,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				ClientSoftwareName:    "", // No client software fields in version 2
				ClientSoftwareVersion: "",
				TaggedFields:          map[string]string{},
			},
			want: ApiVersionsResponse{
				CorrelationId: 123,
				ErrorCode:     0,
				ApiKeys: []ApiVersion{
					{ApiKey: 18, MinVersion: 0, MaxVersion: 4, TaggedFields: map[string]string{}},
				},
				ThrottleTime: 0,
				TaggedFields: map[string]string{},
			},
		},
		{
			name: "Invalid request version 2 (non-flexible) - should return error code 42",
			request: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       16,
					RequestApiKey:     18,
					RequestApiVersion: 2,
					CorrelationId:     123,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				ClientSoftwareName:    "go-cli",
				ClientSoftwareVersion: "1.2.3",
				TaggedFields:          map[string]string{},
			},
			want: ApiVersionsResponse{
				CorrelationId: 123,
				ErrorCode:     42,
				ApiKeys: []ApiVersion{
					{ApiKey: 18, MinVersion: 0, MaxVersion: 4, TaggedFields: map[string]string{}},
				},
				ThrottleTime: 0,
				TaggedFields: map[string]string{},
			},
		},
		{
			name: "Invalid version - should return error code 35",
			request: ApiVersionsRequest{
				Header: RequestHeader{
					MessageSize:       16,
					RequestApiKey:     18,
					RequestApiVersion: 5,
					CorrelationId:     99,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				ClientSoftwareName:    "",
				ClientSoftwareVersion: "",
				TaggedFields:          map[string]string{},
			},
			want: ApiVersionsResponse{
				CorrelationId: 99,
				ErrorCode:     35,
				ApiKeys: []ApiVersion{
					{ApiKey: 18, MinVersion: 0, MaxVersion: 4, TaggedFields: map[string]string{}},
				},
				ThrottleTime: 0,
				TaggedFields: map[string]string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := handler.Handle(&tt.request)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			gotResp, ok := got.(*ApiVersionsResponse)
			if !ok {
				t.Errorf("expected *ApiVersionsResponse, got %T", got)
				return
			}

			if gotResp.CorrelationId != tt.want.CorrelationId {
				t.Errorf("CorrelationId mismatch: got %d, want %d", gotResp.CorrelationId, tt.want.CorrelationId)
			}

			if gotResp.ErrorCode != tt.want.ErrorCode {
				t.Errorf("ErrorCode mismatch: got %d, want %d", gotResp.ErrorCode, tt.want.ErrorCode)
			}

			if gotResp.ThrottleTime != tt.want.ThrottleTime {
				t.Errorf("ThrottleTime mismatch: got %d, want %d", gotResp.ThrottleTime, tt.want.ThrottleTime)
			}

			if len(gotResp.ApiKeys) != len(tt.want.ApiKeys) {
				t.Errorf("ApiKeys length mismatch: got %d, want %d", len(gotResp.ApiKeys), len(tt.want.ApiKeys))
			}

			for i, gotApiKey := range gotResp.ApiKeys {
				wantApiKey := tt.want.ApiKeys[i]

				if gotApiKey.ApiKey != wantApiKey.ApiKey {
					t.Errorf("ApiKeys[%d].ApiKey mismatch: got %d, want %d", i, gotApiKey.ApiKey, wantApiKey.ApiKey)
				}
				if gotApiKey.MinVersion != wantApiKey.MinVersion {
					t.Errorf("ApiKeys[%d].MinVersion mismatch: got %d, want %d", i, gotApiKey.MinVersion, wantApiKey.MinVersion)
				}
				if gotApiKey.MaxVersion != wantApiKey.MaxVersion {
					t.Errorf("ApiKeys[%d].MaxVersion mismatch: got %d, want %d", i, gotApiKey.MaxVersion, wantApiKey.MaxVersion)
				}

				if gotApiKey.TaggedFields == nil {
					gotApiKey.TaggedFields = map[string]string{}
				}
				if wantApiKey.TaggedFields == nil {
					wantApiKey.TaggedFields = map[string]string{}
				}

				if !reflect.DeepEqual(gotApiKey.TaggedFields, wantApiKey.TaggedFields) {
					t.Errorf("ApiKeys[%d].TaggedFields mismatch: got %v, want %v", i, gotApiKey.TaggedFields, wantApiKey.TaggedFields)
				}
			}

			if gotResp.TaggedFields == nil {
				gotResp.TaggedFields = map[string]string{}
			}
			if tt.want.TaggedFields == nil {
				tt.want.TaggedFields = map[string]string{}
			}

			if !reflect.DeepEqual(gotResp.TaggedFields, tt.want.TaggedFields) {
				t.Errorf("TaggedFields mismatch: got %v, want %v", gotResp.TaggedFields, tt.want.TaggedFields)
			}
		})
	}
}

func BenchmarkResponseSerialize(b *testing.B) {
	response := ApiVersionsResponse{
		CorrelationId: 66,
		ErrorCode:     0,
		ApiKeys: []ApiVersion{
			{ApiKey: 18, MinVersion: 0, MaxVersion: 4},
		},
		ThrottleTime: 0,
		TaggedFields: make(map[string]string),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := response.Serialize(4)
		if err != nil {
			b.Fatal(err)
		}
	}
}
