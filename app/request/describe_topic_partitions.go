package request

import (
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/parser"
	"github.com/codecrafters-io/kafka-starter-go/app/serializer"
)

type Topic struct {
	Name         string
	TaggedFields map[string]string
}

type Cursor struct {
	TopicName      string
	PartitionIndex int32
	TaggedFields   map[string]string
}

type DescribeTopicPartitionsRequest struct {
	Header                 RequestHeader
	Topics                 []Topic
	ResponsePartitionLimit int32
	Cursor                 *Cursor
	TaggedFields           map[string]string
}

func (r *DescribeTopicPartitionsRequest) GetHeader() RequestHeader {
	return r.Header
}

func (r *DescribeTopicPartitionsRequest) GetApiKey() KafkaAPIKey {
	return DescribeTopicPartitions
}

func (r *DescribeTopicPartitionsRequest) GetApiVersion() int16 {
	return r.Header.RequestApiVersion
}

func (r *DescribeTopicPartitionsRequest) Validate() error {
	if r.Header.RequestApiVersion != 0 {
		return &RequestParseError{Code: UNSUPPORTED_VERSION, Message: "Invalid version"}
	}

	return nil
}

type Partition struct {
	ErrorCode              int16
	Index                  int32
	LeaderId               int32
	LeaderEpoch            int32
	ReplicaNodes           int32
	IsrNodes               int32
	EligibleLeaderReplicas int32
	LastKnownELR           int32
	OfflineReplicas        int32
	TaggedFields           map[string]string
}

type ResponseTopic struct {
	ErrorCode                 int16
	Name                      string
	Id                        string
	IsInternal                bool
	Partitions                []Partition
	TopicAuthorizedOperations int32
	TaggedFields              map[string]string
}

type DescribeTopicPartitionsResponse struct {
	CorrelationId int32
	ThrottleTime  int32
	Topics        []ResponseTopic
	NextCursor    *Cursor
	TaggedFields  map[string]string
}

func (r *DescribeTopicPartitionsResponse) GetCorrelationId() int32 { return r.CorrelationId }

func (r *DescribeTopicPartitionsResponse) Serialize(apiVersion int16) ([]byte, error) {
	buffer := make([]byte, 256)
	index := 0
	var err error

	// Message size (placeholder)
	index += 4

	index, err = serializer.SerializeInt32(buffer, index, r.CorrelationId)
	if err != nil {
		return nil, err
	}

	// These are the response header tagged fields but for simplicity I am just copying the ones from
	// the response body
	index, err = serializer.SerializeTaggedFields(buffer, index, r.TaggedFields)
	if err != nil {
		return nil, err
	}

	index, err = serializer.SerializeInt32(buffer, index, r.ThrottleTime)
	if err != nil {
		return nil, err
	}

	index, err = serializer.SerializeUnsignedVarInt(buffer, index, uint64(len(r.Topics)+1))
	if err != nil {
		return nil, err
	}

	for _, topic := range r.Topics {
		index, err = serializer.SerializeInt16(buffer, index, topic.ErrorCode)
		if err != nil {
			return nil, err
		}

		index, err = serializer.SerializeCompactNullableString(buffer, index, &topic.Name)
		if err != nil {
			return nil, err
		}

		index, err = serializer.SerializeUUID(buffer, index, topic.Id)
		if err != nil {
			return nil, err
		}

		index, err = serializer.SerializeBoolean(buffer, index, topic.IsInternal)
		if err != nil {
			return nil, err
		}

		index, err = serializer.SerializeUnsignedVarInt(buffer, index, uint64(len(topic.Partitions)+1))
		if err != nil {
			return nil, err
		}

		for _, item := range topic.Partitions {
			index, err = serializer.SerializeInt16(buffer, index, item.ErrorCode)
			if err != nil {
				return nil, err
			}

			index, err = serializer.SerializeInt32(buffer, index, item.Index)
			if err != nil {
				return nil, err
			}

			index, err = serializer.SerializeInt32(buffer, index, item.LeaderId)
			if err != nil {
				return nil, err
			}

			index, err = serializer.SerializeInt32(buffer, index, item.LeaderEpoch)
			if err != nil {
				return nil, err
			}

			index, err = serializer.SerializeInt32(buffer, index, item.ReplicaNodes)
			if err != nil {
				return nil, err
			}

			index, err = serializer.SerializeInt32(buffer, index, item.IsrNodes)
			if err != nil {
				return nil, err
			}

			index, err = serializer.SerializeInt32(buffer, index, item.EligibleLeaderReplicas)
			if err != nil {
				return nil, err
			}

			index, err = serializer.SerializeInt32(buffer, index, item.LastKnownELR)
			if err != nil {
				return nil, err
			}

			index, err = serializer.SerializeInt32(buffer, index, item.OfflineReplicas)
			if err != nil {
				return nil, err
			}

			index, err = serializer.SerializeTaggedFields(buffer, index, r.TaggedFields)
			if err != nil {
				return nil, err
			}
		}

		index, err = serializer.SerializeInt32(buffer, index, topic.TopicAuthorizedOperations)
		if err != nil {
			return nil, err
		}

		index, err = serializer.SerializeTaggedFields(buffer, index, r.TaggedFields)
		if err != nil {
			return nil, err
		}
	}

	if r.NextCursor == nil {
		index, err = serializer.SerializeInt8(buffer, index, -1)
		if err != nil {
			return nil, err
		}
	} else {
		index, err = serializer.SerializeInt8(buffer, index, 1)
		if err != nil {
			return nil, err
		}

		index, err = serializer.SerializeCompactString(buffer, index, r.NextCursor.TopicName)
		if err != nil {
			return nil, err
		}

		index, err = serializer.SerializeInt32(buffer, index, r.NextCursor.PartitionIndex)
		if err != nil {
			return nil, err
		}

		index, err = serializer.SerializeTaggedFields(buffer, index, r.NextCursor.TaggedFields)
		if err != nil {
			return nil, err
		}
	}

	index, err = serializer.SerializeTaggedFields(buffer, index, r.TaggedFields)
	if err != nil {
		return nil, err
	}

	// Fill in message size
	binary.BigEndian.PutUint32(buffer[0:4], uint32(index-4))

	return buffer[:index], nil
}

type DescribeTopicPartitionsHandler struct{}

func (h *DescribeTopicPartitionsHandler) ParseRequestBody(requestHeader RequestHeader, buffer []byte, index int) (KafkaRequest, error) {
	var err error
	req := &DescribeTopicPartitionsRequest{}
	req.Header = requestHeader

	arrayLength, index, err := parser.ExtractUnsignedVarInt(buffer, index)
	if err != nil {
		return nil, &RequestParseError{
			Code:    INVALID_REQUEST,
			Message: "Failed to parse topics length from DescribeTopicPartitions request",
		}
	}

	topicsLength := arrayLength - 1
	topics := make([]Topic, 0, topicsLength)

	for i := 0; i < int(topicsLength); i++ {
		topic := Topic{}

		topic.Name, index, err = parser.ExtractCompactString(buffer, index)
		if err != nil {
			return nil, &RequestParseError{
				Code:    INVALID_REQUEST,
				Message: fmt.Sprintf("Failed to parse topic name from DescribeTopicPartitions request at index %d", i),
			}
		}

		topic.TaggedFields, index, err = parser.ExtractTagFields(buffer, index)
		if err != nil {
			return nil, &RequestParseError{
				Code:    INVALID_REQUEST,
				Message: "Failed to parse tagged fields from DescribeTopicPartitions request",
			}
		}

		topics = append(topics, topic)
	}

	req.Topics = topics

	req.ResponsePartitionLimit, index, err = parser.ExtractInt32(buffer, index)
	if err != nil {
		return nil, &RequestParseError{
			Code:    INVALID_REQUEST,
			Message: "Failed to parse response partition limit from DescribeTopicPartitions request",
		}
	}

	// Parse cursor - starts with INT8: 0xFF (-1) = null, 0x01 (1) = non-null
	var cursorPresence int8
	cursorPresence, index, err = parser.ExtractInt8(buffer, index)
	if err != nil {
		return nil, &RequestParseError{
			Code:    INVALID_REQUEST,
			Message: "Failed to parse cursor presence from DescribeTopicPartitions request",
		}
	}

	switch cursorPresence {
	case -1:
		req.Cursor = nil
	case 1:
		cursor := Cursor{}

		cursor.TopicName, index, err = parser.ExtractCompactString(buffer, index)
		if err != nil {
			return nil, &RequestParseError{
				Code:    INVALID_REQUEST,
				Message: "Failed to parse topic name for cursor from DescribeTopicPartitions request",
			}
		}

		cursor.PartitionIndex, index, err = parser.ExtractInt32(buffer, index)
		if err != nil {
			return nil, &RequestParseError{
				Code:    INVALID_REQUEST,
				Message: "Failed to parse partition index for cursor from DescribeTopicPartitions request",
			}
		}

		cursor.TaggedFields, index, err = parser.ExtractTagFields(buffer, index)
		if err != nil {
			return nil, &RequestParseError{
				Code:    INVALID_REQUEST,
				Message: "Failed to parse tagged fields for cursor from DescribeTopicPartitions request",
			}
		}

		req.Cursor = &cursor
	default:
		return nil, &RequestParseError{
			Code:    INVALID_REQUEST,
			Message: fmt.Sprintf("Invalid cursor presence value: expected 0xFF (null) or 0x01 (non-null), got 0x%02X", uint8(cursorPresence)),
		}
	}

	req.TaggedFields, _, err = parser.ExtractTagFields(buffer, index)
	if err != nil {
		return nil, &RequestParseError{
			Code:    INVALID_REQUEST,
			Message: "Failed to parse tagged fields from DescribeTopicPartitions request",
		}
	}

	return req, nil
}

func (h *DescribeTopicPartitionsHandler) Handle(req KafkaRequest) (KafkaResponse, error) {
	apiReq, ok := req.(*DescribeTopicPartitionsRequest)
	if !ok {
		return nil, fmt.Errorf("DescribeTopicPartitionsHandler received %T instead of *DescribeTopicPartitionsRequest", req)
	}

	// err := apiReq.Validate()
	// var errorCode int16 = 0

	// if err != nil {
	// 	if reqError, ok := err.(*RequestParseError); ok {
	// 		errorCode = int16(reqError.Code)
	// 	} else {
	// 		errorCode = int16(UNKNOWN)
	// 	}
	// }

	var topics []ResponseTopic

	topic := ResponseTopic{
		ErrorCode:                 int16(UNKNOWN_TOPIC_OR_PARTITION),
		Name:                      apiReq.Topics[0].Name,
		Id:                        "00000000-0000-0000-0000-000000000000",
		IsInternal:                false,
		Partitions:                []Partition{},
		TopicAuthorizedOperations: 0,
		TaggedFields:              apiReq.Topics[0].TaggedFields,
	}
	topics = append(topics, topic)

	response := &DescribeTopicPartitionsResponse{
		CorrelationId: apiReq.Header.CorrelationId,
		ThrottleTime:  0,
		Topics:        topics,
		NextCursor:    nil,
		TaggedFields:  make(map[string]string),
	}

	return response, nil
}
