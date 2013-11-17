package balancer

import (
	"time"
)

type queue interface {
	init(string, []string, int, int) queue
	enqueue(request Request) error
	dequeue(num int, blockingTime time.Duration) (requests []Request)
}
