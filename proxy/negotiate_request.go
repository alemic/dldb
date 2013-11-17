package proxy

type dldbNegotiateRequest struct {
	partition     int
	clientRequest *dldbClientRequest
}

func initNegotiateRequest(partition int, clientRequest *dldbClientRequest) *dldbNegotiateRequest {
	request := new(negotiateRequest)
	request.partition = partition
	request.clientRequest = clientRequest
	return request
}
