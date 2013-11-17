package balancer

import (
	"github.com/senarukana/dldb/log"
	"time"
)

type worker struct {
	index      int
	identifier string // for identifier queue
	handler    EventHandler
	pool       *routinePool
	batchChan  chan batch
	removeChan chan *worker
	// requests in the queue
	pending  int
	isForced bool
}

func (self *worker) work() {
	// will not quit until one job has done
	for !self.isForced {
		if self.identifier == "" {
			// max idle time
			tick := time.After(self.pool.routineMaxIdleTime)
			select {
			case _ = <-tick:
				// idle for a long time
				// inform the manager to remove me and return
				if ok := self.pool.routineStoped(self); ok {
					return
				}
				// the routine need to maintain
			case batcher := <-self.batchChan:
				log.Trace("[%s] worker is doing the job, batch num is %d", self.pool.name, batcher.requestNum())
				if batcher.requestNum() == 1 {
					self.handler.EventHandle(batcher.get()[0])
				} else {
					self.handler.EventsHandle(batcher.get())
				}
				// inform the balancer one job has done
				batcher.done(self)
			}
		} else {
			// identifier worker will not quit
			for batcher := range self.batchChan {
				log.Trace("[%s] worker is doing the job, batch num is %d", self.identifier, batcher.requestNum())
				if batcher.requestNum() == 1 {
					self.handler.EventHandle(batcher.get()[0])
				} else {
					self.handler.EventsHandle(batcher.get())
				}
				// inform the balancer one job has done
				batcher.done(self)
			}
		}
	}
	// if isForced is true, we should do nothing and return
}
