package request

import "github.com/codecrafters-io/kafka-starter-go/app/parsing"

type RequestHeader struct {
	MessageSize       int32
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientId          string
	TaggedFields      map[string]string
}

func ParseRequestHeader(buffer []byte, index int) (RequestHeader, int, error) {
	var requestHeader RequestHeader
	var err error

	requestHeader.MessageSize, index, err = parsing.ExtractInt32(buffer, index)
	if err != nil {
		return RequestHeader{}, index, &RequestParseError{
			Code:    INVALID_REQUEST,
			Message: "Failed to parse message size from request header",
		}
	}

	requestHeader.RequestApiKey, index, err = parsing.ExtractInt16(buffer, index)
	if err != nil {
		return RequestHeader{}, index, &RequestParseError{
			Code:    INVALID_REQUEST,
			Message: "Failed to parse API key from request header",
		}
	}

	requestHeader.RequestApiVersion, index, err = parsing.ExtractInt16(buffer, index)
	if err != nil {
		return RequestHeader{}, index, &RequestParseError{
			Code:    INVALID_REQUEST,
			Message: "Failed to parse API version from request header",
		}
	}

	requestHeader.CorrelationId, index, err = parsing.ExtractInt32(buffer, index)
	if err != nil {
		return RequestHeader{}, index, &RequestParseError{
			Code:    INVALID_REQUEST,
			Message: "Failed to parse correlation ID from request header",
		}
	}

	requestHeader.ClientId, index, err = parsing.ExtractNullableString(buffer, index)
	if err != nil {
		return RequestHeader{}, index, &RequestParseError{
			Code:    INVALID_REQUEST,
			Message: "Failed to parse client ID from request header",
		}
	}

	if isFlexibleVersion(requestHeader.RequestApiKey, requestHeader.RequestApiVersion) {
		requestHeader.TaggedFields, index, err = parsing.ExtractTagFields(buffer, index)
		if err != nil {
			return RequestHeader{}, index, &RequestParseError{
				Code:    INVALID_REQUEST,
				Message: "Failed to parse tagged fields from request header",
			}
		}
	}

	return requestHeader, index, nil
}
