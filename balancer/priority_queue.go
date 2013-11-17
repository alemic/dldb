package balancer

import (
	"time"
)

type priorityQueue struct {
	name        string
	requestChan chan Request
	queueLimits int
}

func (self *priorityQueue) init(balancerName string, identifiers []string, queueBuffer int, queueLimits int) queue {
	self.queueLimits = queueLimits
	if queueBuffer != -1 {
		self.requestChan = make(chan Request, queueBuffer)
	} else {
		// infinite queue
		self.requestChan = make(chan Request)
	}
	return self
}

func (self *priorityQueue) enqueue(request Request) error {
	if self.queueLimits != -1 && len(self.requestChan) > self.queueLimits {
		return errTooManyRequests
	}
	self.requestChan <- request
	return nil
}

func (self *priorityQueue) dequeue(num int, blockingTime time.Duration) (requests []Request) {
	n := 0
	timeChan := time.After(blockingTime)
	for n < num {
		select {
		case _ = <-timeChan:
			return requests
		case request := <-self.requestChan:
			requests = append(requests, request)
			n++
		}
	}
	return requests
}
