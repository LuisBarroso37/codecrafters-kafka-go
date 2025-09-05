package request

import (
	"bytes"
	"testing"
)

func TestProcessRequest(t *testing.T) {
	buffer := []byte{
		0x00, 0x00, 0x00, 0x18, // MessageSize: 24
		0x00, 0x12, // RequestApiKey: 18 (ApiVersions)
		0x00, 0x04, // RequestApiVersion: 4 (flexible)
		0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
		0x00, 0x04, // ClientId length: 4
		't', 'e', 's', 't', // ClientId: "test"
		0x01,          // Number of tagged fields (varint, 1)
		0x00,          // Tag ID (varint, 0)
		0x04,          // Value length (varint, 4)
		'b', 'a', 'r', // Value: "bar"
		0x07, 'g', 'o', '-', 'c', 'l', 'i', // clientSoftwareName: "go-cli"
		0x06, '1', '.', '2', '.', '3', // clientSoftwareVersion: "1.2.3"
		0x01,                    // Number of tagged fields (varint, 1)
		0x01,                    // Tag ID (varint, 1)
		0x06,                    // Value length (varint, 6)
		'v', 'a', 'l', 'u', 'e', // Value: "value"
	}
	expected_response := []byte{
		0x00, 0x00, 0x00, 0x13, // MessageSize: 19 (message size excluding this 4-byte field)
		0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
		0x00, 0x00, // ErrorCode: 0 (no error)
		0x02,       // ApiKeys array length: 2 (varint, 1 API + 1 for flexible format)
		0x00, 0x12, // ApiKey: 18 (ApiVersions)
		0x00, 0x00, // MinVersion: 0
		0x00, 0x04, // MaxVersion: 4
		0x00,                   // TaggedFields for ApiKey: 0 (no tagged fields)
		0x00, 0x00, 0x00, 0x00, // ThrottleTime: 0 (4 bytes)
		0x00, // TaggedFields for response: 0 (no tagged fields)
	}
	broker := NewKafkaBroker()

	response, err := broker.ProcessRequest(buffer)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(response, expected_response) {
		t.Errorf("response mismatch:\ngot  %v\nwant %v", response, expected_response)
	}
}

func BenchmarkProcessRequest(b *testing.B) {
	buffer := []byte{
		0x00, 0x00, 0x00, 0x18, // MessageSize: 24
		0x00, 0x12, // RequestApiKey: 18 (ApiVersions)
		0x00, 0x04, // RequestApiVersion: 4 (flexible)
		0x00, 0x00, 0x00, 0x42, // CorrelationId: 66
		0x00, 0x04, // ClientId length: 4
		't', 'e', 's', 't', // ClientId: "test"
		0x01,          // Number of tagged fields (varint, 1)
		0x00,          // Tag ID (varint, 0)
		0x04,          // Value length (varint, 4)
		'b', 'a', 'r', // Value: "bar"
		0x07, 'g', 'o', '-', 'c', 'l', 'i', // clientSoftwareName: "go-cli"
		0x06, '1', '.', '2', '.', '3', // clientSoftwareVersion: "1.2.3"
		0x01,                    // Number of tagged fields (varint, 1)
		0x01,                    // Tag ID (varint, 1)
		0x06,                    // Value length (varint, 6)
		'v', 'a', 'l', 'u', 'e', // Value: "value"
	}
	broker := NewKafkaBroker()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := broker.ProcessRequest(buffer)
		if err != nil {
			b.Fatal(err)
		}
	}
}
