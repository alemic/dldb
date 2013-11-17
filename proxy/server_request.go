package proxy

type dldbDataServerRequest struct {
	clientRequest *BinaryRequest
	serverHost    string
	responseChan  chan *dldbDataServerResponse
}

func initDataServerRequest(clientRequest *BinaryRequest, serverHost string,
	responseChan chan *dldbDataServerResponse) *dldbNegotiateRequest {
	request := new(dldbDataServerRequest)
	request.clientRequest = clientRequest
	request.serverHost = serverHost
	request.responseChan = responseChan
	return request
}
