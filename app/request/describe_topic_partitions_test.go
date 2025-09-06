package request

import (
	"reflect"
	"testing"
)

func TestDescribeTopicPartitionsParseRequestBody(t *testing.T) {
	handler := DescribeTopicPartitionsHandler{}
	baseHeader := RequestHeader{
		MessageSize:   17,
		RequestApiKey: 75,
		CorrelationId: 66,
		ClientId:      "test",
		TaggedFields:  map[string]string{},
	}

	tests := []struct {
		name      string
		version   int16
		input     []byte
		bodyIndex int
		want      DescribeTopicPartitionsRequest
		wantErr   bool
	}{
		{
			name:    "DescribeTopicPartitions request with single topic",
			version: 0,
			input: []byte{
				// Header
				0x00, 0x00, 0x00, 0x20, // MessageSize: 32
				0x00, 0x4B, // RequestApiKey: 75 (DescribeTopicPartitions)
				0x00, 0x00, // RequestApiVersion: 0
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
				0x00, // Number of header tagged fields (varint, 0)
				// Body starts here
				0x02,                     // Topics array length (1 topic + 1)
				0x05, 't', 'e', 's', 't', // Topic name: "test" (compact string: length 4+1=5)
				0x00,                   // Topic tagged fields (varint, 0)
				0x00, 0x00, 0x00, 0x0A, // ResponsePartitionLimit: 10
				0xFF, // Cursor (null - INT8: -1)
				0x00, // Request tagged fields (varint, 0)
			},
			bodyIndex: 19,
			want: DescribeTopicPartitionsRequest{
				Header: RequestHeader{
					MessageSize:       32,
					RequestApiKey:     75,
					RequestApiVersion: 0,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				Topics: []Topic{
					{
						Name:         "test",
						TaggedFields: map[string]string{},
					},
				},
				ResponsePartitionLimit: 10,
				Cursor:                 nil,
				TaggedFields:           map[string]string{},
			},
			wantErr: false,
		},
		{
			name:    "DescribeTopicPartitions request with multiple topics",
			version: 0,
			input: []byte{
				// Header
				0x00, 0x00, 0x00, 0x28, // MessageSize: 40
				0x00, 0x4B, // RequestApiKey: 75
				0x00, 0x00, // RequestApiVersion: 0
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
				0x00, // Number of header tagged fields (varint, 0)
				// Body
				0x03,                     // Topics array length (2 topics + 1)
				0x05, 't', 'e', 's', 't', // Topic 1 name: "test"
				0x00,                // Topic 1 tagged fields
				0x04, 'f', 'o', 'o', // Topic 2 name: "foo"
				0x00,                   // Topic 2 tagged fields
				0x00, 0x00, 0x00, 0x14, // ResponsePartitionLimit: 20
				0xFF, // Cursor (null - INT8: -1)
				0x00, // Request tagged fields
			},
			bodyIndex: 19,
			want: DescribeTopicPartitionsRequest{
				Header: RequestHeader{
					MessageSize:       40,
					RequestApiKey:     75,
					RequestApiVersion: 0,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				Topics: []Topic{
					{Name: "test", TaggedFields: map[string]string{}},
					{Name: "foo", TaggedFields: map[string]string{}},
				},
				ResponsePartitionLimit: 20,
				Cursor:                 nil,
				TaggedFields:           map[string]string{},
			},
			wantErr: false,
		},
		{
			name:    "DescribeTopicPartitions request with topic tagged fields only",
			version: 0,
			input: []byte{
				// Header
				0x00, 0x00, 0x00, 0x25, // MessageSize: 37
				0x00, 0x4B, // RequestApiKey: 75
				0x00, 0x00, // RequestApiVersion: 0
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
				0x00, // Number of header tagged fields (varint, 0)
				// Body
				0x02,                     // Topics array length (1 topic + 1)
				0x05, 't', 'e', 's', 't', // Topic name: "test" (compact string: length 4+1=5)
				0x01,           // Topic tagged fields count (1 field, encoded as varint 1)
				0x01,           // Tag ID 1 (varint)
				0x03, 'h', 'i', // Tag value: "hi" (compact string: length 2+1=3)
				0x00, 0x00, 0x00, 0x0A, // ResponsePartitionLimit: 10
				0xFF, // Cursor not present (INT8: -1)
				0x00, // Request tagged fields (varint, 0)
			},
			bodyIndex: 19,
			want: DescribeTopicPartitionsRequest{
				Header: RequestHeader{
					MessageSize:       37,
					RequestApiKey:     75,
					RequestApiVersion: 0,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				Topics: []Topic{
					{
						Name:         "test",
						TaggedFields: map[string]string{"1": "hi"},
					},
				},
				ResponsePartitionLimit: 10,
				Cursor:                 nil,
				TaggedFields:           map[string]string{},
			},
			wantErr: false,
		},
		{
			name:    "DescribeTopicPartitions request with request tagged fields only",
			version: 0,
			input: []byte{
				// Header
				0x00, 0x00, 0x00, 0x22, // MessageSize: 34
				0x00, 0x4B, // RequestApiKey: 75
				0x00, 0x00, // RequestApiVersion: 0
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
				0x00, // Number of header tagged fields (varint, 0)
				// Body
				0x02,                     // Topics array length (1 topic + 1)
				0x05, 't', 'e', 's', 't', // Topic name: "test" (compact string: length 4+1=5)
				0x00,                   // Topic tagged fields (varint, 0)
				0x00, 0x00, 0x00, 0x0A, // ResponsePartitionLimit: 10
				0xFF,           // Cursor not present (INT8: -1)
				0x01,           // Request tagged fields count (1 field, encoded as varint 1)
				0x01,           // Tag ID 1 (varint)
				0x03, 'h', 'i', // Tag value: "hi" (compact string: length 2+1=3)
			},
			bodyIndex: 19,
			want: DescribeTopicPartitionsRequest{
				Header: RequestHeader{
					MessageSize:       34,
					RequestApiKey:     75,
					RequestApiVersion: 0,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				Topics: []Topic{
					{
						Name:         "test",
						TaggedFields: map[string]string{},
					},
				},
				ResponsePartitionLimit: 10,
				Cursor:                 nil,
				TaggedFields:           map[string]string{"1": "hi"},
			},
			wantErr: false,
		},
		{
			name:    "DescribeTopicPartitions request with cursor and tagged fields",
			version: 0,
			input: []byte{
				// Header
				0x00, 0x00, 0x00, 0x20, // MessageSize: 32 (updated for null cursor)
				0x00, 0x4B, // RequestApiKey: 75
				0x00, 0x00, // RequestApiVersion: 0
				0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
				0x00, 0x04, // ClientId length: 4
				't', 'e', 's', 't', // ClientId: "test"
				0x00, // Number of header tagged fields (varint, 0)
				// Body
				0x02,                     // Topics array length (1 topic + 1)
				0x05, 't', 'e', 's', 't', // Topic name: "test" (compact string: length 4+1=5)
				0x01,           // Topic tagged fields count (1 field, encoded as varint 1)
				0x01,           // Tag ID 1 (varint)
				0x03, 'h', 'i', // Tag value: "hi" (compact string: length 2+1=3)
				0x00, 0x00, 0x00, 0x0A, // ResponsePartitionLimit: 10
				0xFF,                     // Cursor null (INT8: -1)
				0x01,                     // Request tagged fields count (1 field, encoded as varint 1)
				0x02,                     // Tag ID 2 (varint)
				0x05, 'g', 'o', 'o', 'd', // Tag value: "good" (compact string: length 4+1=5)
			},
			bodyIndex: 19,
			want: DescribeTopicPartitionsRequest{
				Header: RequestHeader{
					MessageSize:       32,
					RequestApiKey:     75,
					RequestApiVersion: 0,
					CorrelationId:     66,
					ClientId:          "test",
					TaggedFields:      map[string]string{},
				},
				Topics: []Topic{
					{
						Name:         "test",
						TaggedFields: map[string]string{"1": "hi"},
					},
				},
				ResponsePartitionLimit: 10,
				Cursor:                 nil,
				TaggedFields:           map[string]string{"2": "good"},
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

			dtpReq, ok := got.(*DescribeTopicPartitionsRequest)
			if !ok {
				t.Errorf("DescribeTopicPartitionsHandler received %T instead of *DescribeTopicPartitionsRequest", got)
				return
			}

			if len(dtpReq.Topics) != len(tt.want.Topics) {
				t.Errorf("Topics length mismatch: got %d, want %d", len(dtpReq.Topics), len(tt.want.Topics))
				return
			}

			for i, gotTopic := range dtpReq.Topics {
				wantTopic := tt.want.Topics[i]
				if gotTopic.Name != wantTopic.Name {
					t.Errorf("Topics[%d].Name mismatch: got %q, want %q", i, gotTopic.Name, wantTopic.Name)
				}

				if gotTopic.TaggedFields == nil {
					gotTopic.TaggedFields = map[string]string{}
				}
				if wantTopic.TaggedFields == nil {
					wantTopic.TaggedFields = map[string]string{}
				}

				if !reflect.DeepEqual(gotTopic.TaggedFields, wantTopic.TaggedFields) {
					t.Errorf("Topics[%d].TaggedFields mismatch: got %v, want %v", i, gotTopic.TaggedFields, wantTopic.TaggedFields)
				}
			}

			if dtpReq.ResponsePartitionLimit != tt.want.ResponsePartitionLimit {
				t.Errorf("ResponsePartitionLimit mismatch: got %d, want %d", dtpReq.ResponsePartitionLimit, tt.want.ResponsePartitionLimit)
			}

			// Compare Cursor
			if (dtpReq.Cursor == nil) != (tt.want.Cursor == nil) {
				t.Errorf("Cursor nullability mismatch: got %v, want %v", dtpReq.Cursor == nil, tt.want.Cursor == nil)
			}

			if dtpReq.Cursor != nil && tt.want.Cursor != nil {
				if dtpReq.Cursor.TopicName != tt.want.Cursor.TopicName {
					t.Errorf("Cursor.TopicName mismatch: got %q, want %q", dtpReq.Cursor.TopicName, tt.want.Cursor.TopicName)
				}
				if dtpReq.Cursor.PartitionIndex != tt.want.Cursor.PartitionIndex {
					t.Errorf("Cursor.PartitionIndex mismatch: got %d, want %d", dtpReq.Cursor.PartitionIndex, tt.want.Cursor.PartitionIndex)
				}
			}

			if dtpReq.TaggedFields == nil {
				dtpReq.TaggedFields = map[string]string{}
			}
			if tt.want.TaggedFields == nil {
				tt.want.TaggedFields = map[string]string{}
			}

			if !reflect.DeepEqual(dtpReq.TaggedFields, tt.want.TaggedFields) {
				t.Errorf("TaggedFields mismatch: got %v, want %v", dtpReq.TaggedFields, tt.want.TaggedFields)
			}
		})
	}
}

func TestDescribeTopicPartitionsValidate(t *testing.T) {
	tests := []struct {
		name    string
		req     *DescribeTopicPartitionsRequest
		wantErr bool
	}{
		{
			name: "Valid request with single topic",
			req: &DescribeTopicPartitionsRequest{
				Topics: []Topic{
					{
						Name:         "test-topic",
						TaggedFields: map[string]string{},
					},
				},
				ResponsePartitionLimit: 100,
				Cursor:                 nil,
				TaggedFields:           map[string]string{},
			},
			wantErr: false,
		},
		{
			name: "Valid request with multiple topics",
			req: &DescribeTopicPartitionsRequest{
				Topics: []Topic{
					{
						Name:         "topic1",
						TaggedFields: map[string]string{},
					},
					{
						Name:         "topic2",
						TaggedFields: map[string]string{},
					},
				},
				ResponsePartitionLimit: 50,
				Cursor:                 nil,
				TaggedFields:           map[string]string{},
			},
			wantErr: false,
		},
		{
			name: "Valid request with cursor",
			req: &DescribeTopicPartitionsRequest{
				Topics: []Topic{
					{
						Name:         "topic-with-cursor",
						TaggedFields: map[string]string{},
					},
				},
				ResponsePartitionLimit: 200,
				Cursor: &Cursor{
					TopicName:      "previous-topic",
					PartitionIndex: 5,
				},
				TaggedFields: map[string]string{},
			},
			wantErr: false,
		},
		{
			name: "Valid request with tagged fields",
			req: &DescribeTopicPartitionsRequest{
				Topics: []Topic{
					{
						Name:         "tagged-topic",
						TaggedFields: map[string]string{"tag1": "value1"},
					},
				},
				ResponsePartitionLimit: 75,
				Cursor:                 nil,
				TaggedFields:           map[string]string{"global": "value"},
			},
			wantErr: false,
		},
		{
			name: "Empty topics list",
			req: &DescribeTopicPartitionsRequest{
				Topics:                 []Topic{},
				ResponsePartitionLimit: 100,
				Cursor:                 nil,
				TaggedFields:           map[string]string{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()

			if tt.wantErr && err == nil {
				t.Errorf("expected error but got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestDescribeTopicPartitionsHandleRequest(t *testing.T) {
	handler := DescribeTopicPartitionsHandler{}

	tests := []struct {
		name    string
		request DescribeTopicPartitionsRequest
		want    DescribeTopicPartitionsResponse
	}{
		{
			name: "Unknown topic",
			request: DescribeTopicPartitionsRequest{
				Header: RequestHeader{
					MessageSize:       32,
					RequestApiKey:     75,
					RequestApiVersion: 0,
					CorrelationId:     123,
					ClientId:          "test-client",
					TaggedFields:      map[string]string{},
				},
				Topics: []Topic{
					{
						Name:         "test-topic",
						TaggedFields: map[string]string{},
					},
				},
				ResponsePartitionLimit: 100,
				Cursor:                 nil,
				TaggedFields:           map[string]string{},
			},
			want: DescribeTopicPartitionsResponse{
				CorrelationId: 123,
				ThrottleTime:  0,
				Topics: []ResponseTopic{
					{
						ErrorCode:                 3,
						Name:                      "test-topic",
						Id:                        "00000000-0000-0000-0000-000000000000",
						IsInternal:                false,
						Partitions:                []Partition{},
						TopicAuthorizedOperations: 0,
						TaggedFields:              map[string]string{},
					},
				},
				NextCursor:   nil,
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

			gotResp, ok := got.(*DescribeTopicPartitionsResponse)
			if !ok {
				t.Errorf("expected *DescribeTopicPartitionsResponse, got %T", got)
				return
			}

			if gotResp.CorrelationId != tt.want.CorrelationId {
				t.Errorf("CorrelationId mismatch: got %d, want %d", gotResp.CorrelationId, tt.want.CorrelationId)
			}

			if gotResp.ThrottleTime != tt.want.ThrottleTime {
				t.Errorf("ThrottleTime mismatch: got %d, want %d", gotResp.ThrottleTime, tt.want.ThrottleTime)
			}

			if len(gotResp.Topics) != len(tt.want.Topics) {
				t.Errorf("Topics length mismatch: got %d, want %d", len(gotResp.Topics), len(tt.want.Topics))
				return
			}

			for i, gotTopic := range gotResp.Topics {
				wantTopic := tt.want.Topics[i]

				if gotTopic.ErrorCode != wantTopic.ErrorCode {
					t.Errorf("Topic[%d].ErrorCode mismatch: got %d, want %d", i, gotTopic.ErrorCode, wantTopic.ErrorCode)
				}

				if gotTopic.Name != wantTopic.Name {
					t.Errorf("Topic[%d].Name mismatch: got %s, want %s", i, gotTopic.Name, wantTopic.Name)
				}

				if gotTopic.Id != wantTopic.Id {
					t.Errorf("Topic[%d].Id mismatch: got %s, want %s", i, gotTopic.Id, wantTopic.Id)
				}

				if gotTopic.IsInternal != wantTopic.IsInternal {
					t.Errorf("Topic[%d].IsInternal mismatch: got %t, want %t", i, gotTopic.IsInternal, wantTopic.IsInternal)
				}

				if len(gotTopic.Partitions) != len(wantTopic.Partitions) {
					t.Errorf("Topic[%d].Partitions length mismatch: got %d, want %d", i, len(gotTopic.Partitions), len(wantTopic.Partitions))
				}

				if gotTopic.TopicAuthorizedOperations != wantTopic.TopicAuthorizedOperations {
					t.Errorf("Topic[%d].TopicAuthorizedOperations mismatch: got %d, want %d", i, gotTopic.TopicAuthorizedOperations, wantTopic.TopicAuthorizedOperations)
				}

				if gotTopic.TaggedFields == nil {
					gotTopic.TaggedFields = map[string]string{}
				}
				if wantTopic.TaggedFields == nil {
					wantTopic.TaggedFields = map[string]string{}
				}

				if !reflect.DeepEqual(gotTopic.TaggedFields, wantTopic.TaggedFields) {
					t.Errorf("Topic[%d].TaggedFields mismatch: got %v, want %v", i, gotTopic.TaggedFields, wantTopic.TaggedFields)
				}
			}

			if (gotResp.NextCursor == nil) != (tt.want.NextCursor == nil) {
				t.Errorf("NextCursor nullability mismatch: got %v, want %v", gotResp.NextCursor == nil, tt.want.NextCursor == nil)
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
