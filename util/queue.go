package util

/*import (
	"sync"
	"time"
)

type SyncQueue struct {
	data    []interface{}
	mutex   sync.Mutex
	condvar *sync.Cond
}

func InitSyncQueue() *SyncQueue {
	l := new(SyncQueue)
	l.condvar = sync.NewCond(l.mutex)
	return l
}

func (self *SyncQueue) Push(item interface{}) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.data = append(self.data, item)
	return
}

func (self *SyncQueue) BlockPop() []interface{} {

}

func (self *SyncQueue) Pop() interface{} {
	if len(self.data) == 0 {
		return nil
	}
	self.mutex.Lock()
	defer self.mutex.Unlock()
	item := self.data[0]
	self.data = self.data[1:]
	return item
}

func (self *SyncQueue) PopAll() interface{} {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	allData := self.data
	self.data = self.data[:0]
	return allData
}
*/

import (
	"errors"
	"time"
)

type Queue struct {
	requestChan  chan interface{}
	queueLimits  int
	dequeueNum   int
	blockingTime time.Duration
}

func InitQueue(queueBuffer int, queueLimits int, dequeueNum int, blockingTime time.Duration) *Queue {
	queue := new(Queue)
	queue.blockingTime = blockingTime
	queue.queueLimits = queueLimits
	queue.dequeueNum = dequeueNum
	if queueBuffer != -1 {
		queue.requestChan = make(chan interface{}, queueBuffer)
	} else {
		// infinite queue
		queue.requestChan = make(chan interface{})
	}
	return queue
}

func (self *Queue) Enqueue(request interface{}) error {
	if self.queueLimits != -1 && len(self.requestChan) > self.queueLimits {
		return errors.New("Sorry, there are too many requests. Please try again later")
	}
	self.requestChan <- request
	return nil
}

func (self *Queue) Dequeue() (requests []interface{}) {
	n := 0
	timeChan := time.After(self.blockingTime)
	for n < self.dequeueNum {
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
