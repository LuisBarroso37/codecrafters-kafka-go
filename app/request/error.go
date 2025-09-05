package request

import (
	"fmt"
)

type RequestParseError struct {
	Code    KafkaErrorCode
	Message string
}

func (e *RequestParseError) Error() string {
	return fmt.Sprintf("%s: %s", KafkaErrorCodeNames[e.Code], e.Message)
}
