package balancer

import (
	"container/heap"
	_ "fmt"
	"github.com/senarukana/dldb/log"
	"sync"
	"time"
)

//-----------------heap method complete-----------------------

type routinePool struct {
	name               string
	routineNums        int
	minRoutineNum      int
	routineMaxIdleTime time.Duration
	sync.Mutex         // remove routine
	workers            workerPool
	batchChan          chan batch
	handler            EventHandler
}

func initRoutinePool(name string, initRoutineNum int, handler EventHandler, routineMaxIdleTime time.Duration, minRoutineNum int) (pool *routinePool) {
	pool = new(routinePool)
	pool.name = name
	pool.handler = handler
	pool.routineMaxIdleTime = routineMaxIdleTime
	pool.minRoutineNum = minRoutineNum
	pool.addRoutines(initRoutineNum)
	return pool
}

func (self *routinePool) addRoutines(num int) {
	self.routineNums += num
	log.Info("%s, add %d workers, current routine num is %d", self.name, num, self.routineNums)
	for i := 0; i < num; i++ {
		w := new(worker)
		w.handler = self.handler
		w.batchChan = make(chan batch)
		w.pool = self
		w.pending = 0
		w.isForced = false
		heap.Push(&self.workers, w)
		go w.work()
	}
}

// routine has already inform the mgr it stoped
// we should remove it from the pool
func (self *routinePool) routineStoped(w *worker) bool {
	self.Lock()
	defer self.Unlock()
	if self.routineNums > self.minRoutineNum {
		// panic("one worker has stoped")
		self.routineNums--
		log.Info("%s : one worker has stoped, current routine num is %d", self.name, self.routineNums)
		heap.Remove(&self.workers, w.index)
		return true
	} else {
		return false
	}
}

// remove the least active routine
func (self *routinePool) removeRoutines(num int) {
	log.Info("remove %d routines", num)
	self.Lock()
	defer self.Unlock()
	self.routineNums -= num
	for i := 0; i < num; i++ {
		w := heap.Pop(&self.workers).(*worker)
		// inform the routine to stop
		w.isForced = true
	}
}
