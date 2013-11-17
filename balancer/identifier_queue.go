package balancer

import (
	"fmt"
	"time"
)

// each identifer should only have one worker to do the job. worker is distiguished by id
type identifierQueue struct {
	balancerName string
	requestsChan map[string]chan IdentifierRequest
	queueLimits  int
}

func (self *identifierQueue) init(name string, identifiers []string, queueBuffer int, queueLimits int) queue {
	self.balancerName = name
	self.queueLimits = queueLimits
	for _, identifier := range identifiers {
		if queueBuffer != -1 {
			self.requestsChan[identifier] = make(chan IdentifierRequest, queueBuffer)
		} else {
			self.requestsChan[identifier] = make(chan IdentifierRequest)
		}
	}
	return self
}

// assume the request is a identifierRequest, otherwise panic
func (self *identifierQueue) enqueue(request Request) error {
	iRequest, ok := request.(IdentifierRequest)
	if !ok {
		panic(fmt.Sprintf("[%s] receive a non region request", self.balancerName))
	}
	requestChan := self.requestsChan[iRequest.Id]
	if self.queueLimits != -1 && len(requestChan) > self.queueLimits {
		return errTooManyRequests
	}
	requestChan <- iRequest
	return nil
}

// assume the server is little
// return the channel that has the most chan buffer
func (self *identifierQueue) findRequestChan() chan IdentifierRequest {
	max := 0
	var findChannel chan IdentifierRequest
	for _, channel := range self.requestsChan {
		if max == 0 || max < len(channel) {
			max = len(channel)
			findChannel = channel
		}
	}
	return findChannel
}

func (self *identifierQueue) dequeue(num int, blockingTime time.Duration) (requests []Request) {
	n := 0
	timeChan := time.Tick(blockingTime)
	waitChannel := self.findRequestChan()
	for n < num {
		select {
		case _ = <-timeChan:
			return requests
		case request := <-waitChannel:
			requests = append(requests, request)
			n++
		}
	}
	return requests
}
