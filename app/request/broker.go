package request

import "fmt"

type KafkaBroker struct {
	handlers map[KafkaAPIKey]RequestHandler
}

func (b *KafkaBroker) ProcessRequest(buffer []byte) ([]byte, error) {
	index := 0

	requestHeader, index, err := ParseRequestHeader(buffer, index)
	if err != nil {
		return nil, err
	}

	handler, exists := b.handlers[KafkaAPIKey(requestHeader.RequestApiKey)]
	if !exists {
		return nil, &RequestParseError{Code: INVALID_REQUEST, Message: fmt.Sprintf("unsupported API key: %d", requestHeader.RequestApiKey)}
	}

	request, err := handler.ParseRequestBody(requestHeader, buffer, index)
	if err != nil {
		return nil, err
	}

	response, err := handler.Handle(request)
	if err != nil {
		return nil, err
	}

	return response.Serialize(requestHeader.RequestApiVersion)
}

func NewKafkaBroker() KafkaBroker {
	handlers := make(map[KafkaAPIKey]RequestHandler)
	handlers[ApiVersions] = &ApiVersionsHandler{
		supportedApis: []ApiVersion{
			{ApiKey: 18, MinVersion: 0, MaxVersion: 4, TaggedFields: map[string]string{}},
		},
	}

	return KafkaBroker{
		handlers: handlers,
	}
}
