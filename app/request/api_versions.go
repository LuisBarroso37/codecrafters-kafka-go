package request

import (
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/parsing"
)

type ApiVersionsRequest struct {
	Header                RequestHeader
	ClientSoftwareName    string
	ClientSoftwareVersion string
	TaggedFields          map[string]string
}

func (r *ApiVersionsRequest) GetHeader() RequestHeader {
	return r.Header
}

func (r *ApiVersionsRequest) GetApiKey() KafkaAPIKey {
	return ApiVersions
}

func (r *ApiVersionsRequest) GetApiVersion() int16 {
	return r.Header.RequestApiVersion
}

func (r *ApiVersionsRequest) Validate() error {
	if r.Header.RequestApiVersion < 0 || r.Header.RequestApiVersion > 4 {
		return &RequestParseError{Code: UNSUPPORTED_VERSION, Message: "Invalid version"}
	}

	if r.Header.RequestApiVersion >= 3 {
		if r.ClientSoftwareName == "" {
			return &RequestParseError{Code: INVALID_REQUEST, Message: "Client software name is required"}
		}

		if r.ClientSoftwareVersion == "" {
			return &RequestParseError{Code: INVALID_REQUEST, Message: "Client software version is required"}
		}
	} else {
		if r.ClientSoftwareName != "" {
			return &RequestParseError{Code: INVALID_REQUEST, Message: "Client software name must not be set"}
		}

		if r.ClientSoftwareVersion != "" {
			return &RequestParseError{Code: INVALID_REQUEST, Message: "Client software version must not be set"}
		}
	}

	return nil
}

type ApiVersion struct {
	ApiKey       int16
	MinVersion   int16
	MaxVersion   int16
	TaggedFields map[string]string
}

type ApiVersionsResponse struct {
	CorrelationId int32
	ErrorCode     int16
	ApiKeys       []ApiVersion
	ThrottleTime  int32
	TaggedFields  map[string]string
}

func (r *ApiVersionsResponse) GetCorrelationId() int32 { return r.CorrelationId }

func (r *ApiVersionsResponse) GetErrorCode() int16 { return r.ErrorCode }

func (r *ApiVersionsResponse) Serialize(apiVersion int16) ([]byte, error) {
	buffer := make([]byte, 256)
	index := 0

	// Message size (placeholder)
	index += 4

	// Correlation ID
	binary.BigEndian.PutUint32(buffer[index:index+4], uint32(r.CorrelationId))
	index += 4

	// Error code
	binary.BigEndian.PutUint16(buffer[index:index+2], uint16(r.ErrorCode))
	index += 2

	// ApiKeys array length
	if apiVersion >= 3 {
		index += binary.PutUvarint(buffer[index:], uint64(len(r.ApiKeys)+1))
	} else {
		binary.BigEndian.PutUint32(buffer[index:index+4], uint32(len(r.ApiKeys)))
		index += 4
	}

	// ApiKeys
	for _, apiKey := range r.ApiKeys {
		binary.BigEndian.PutUint16(buffer[index:index+2], uint16(apiKey.ApiKey))
		index += 2
		binary.BigEndian.PutUint16(buffer[index:index+2], uint16(apiKey.MinVersion))
		index += 2
		binary.BigEndian.PutUint16(buffer[index:index+2], uint16(apiKey.MaxVersion))
		index += 2

		// Tagged fields
		if apiVersion >= 3 {
			newIndex, err := parsing.SerializeTaggedFields(buffer, index, apiKey.TaggedFields)
			if err != nil {
				return nil, err
			}

			index = newIndex
		}
	}

	// Throttle time
	if apiVersion >= 1 {
		binary.BigEndian.PutUint32(buffer[index:index+4], uint32(r.ThrottleTime))
		index += 4
	}

	// Tagged fields
	if apiVersion >= 3 {
		newIndex, err := parsing.SerializeTaggedFields(buffer, index, r.TaggedFields)
		if err != nil {
			return nil, err
		}

		index = newIndex
	}

	// Fill in message size
	binary.BigEndian.PutUint32(buffer[0:4], uint32(index-4))

	return buffer[:index], nil
}

type ApiVersionsHandler struct {
	supportedApis []ApiVersion
}

func (h *ApiVersionsHandler) ParseRequestBody(requestHeader RequestHeader, buffer []byte, index int) (KafkaRequest, error) {
	var err error
	req := &ApiVersionsRequest{}
	req.Header = requestHeader

	if isFlexibleVersion(req.Header.RequestApiKey, req.Header.RequestApiVersion) {
		req.ClientSoftwareName, index, err = parsing.ExtractCompactString(buffer, index)
		if err != nil {
			return nil, &RequestParseError{
				Code:    INVALID_REQUEST,
				Message: "Failed to parse client software name from ApiVersions request",
			}
		}

		req.ClientSoftwareVersion, index, err = parsing.ExtractCompactString(buffer, index)
		if err != nil {
			return nil, &RequestParseError{
				Code:    INVALID_REQUEST,
				Message: "Failed to parse client software version from ApiVersions request",
			}
		}

		req.TaggedFields, _, err = parsing.ExtractTagFields(buffer, index)
		if err != nil {
			return nil, &RequestParseError{
				Code:    INVALID_REQUEST,
				Message: "Failed to parse tagged fields from ApiVersions request",
			}
		}
	}

	return req, nil
}

func (h *ApiVersionsHandler) Handle(req KafkaRequest) (KafkaResponse, error) {
	apiReq, ok := req.(*ApiVersionsRequest)
	if !ok {
		return nil, fmt.Errorf("ApiVersionsHandler received %T instead of *ApiVersionsRequest", req)
	}

	err := apiReq.Validate()
	var errorCode int16 = 0

	if err != nil {
		if reqError, ok := err.(*RequestParseError); ok {
			errorCode = int16(reqError.Code)
		} else {
			errorCode = int16(UNKNOWN)
		}
	}

	response := &ApiVersionsResponse{
		CorrelationId: apiReq.Header.CorrelationId,
		ErrorCode:     errorCode,
		ApiKeys:       h.supportedApis,
		ThrottleTime:  0,
		TaggedFields:  make(map[string]string),
	}

	return response, nil
}
