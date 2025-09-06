package request

type KafkaRequest interface {
	GetHeader() RequestHeader
	GetApiKey() KafkaAPIKey
	GetApiVersion() int16
	Validate() error
}

type KafkaResponse interface {
	GetCorrelationId() int32
	Serialize(apiVersion int16) ([]byte, error)
}

type RequestHandler interface {
	ParseRequestBody(requestHeader RequestHeader, buffer []byte, index int) (KafkaRequest, error)
	Handle(KafkaRequest) (KafkaResponse, error)
}
