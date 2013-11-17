package balancer

import (
	"github.com/senarukana/dldb/log"
	"sync"
)

type identifierRoutinePool struct {
	name        string
	routineNums int
	sync.Mutex  // remove routine
	workers     workerPool
	batchChan   chan batch
	handler     EventHandler
}

func initIdentifierRoutinePool(name string, handler EventHandler, identifiers []string) *identifierRoutinePool {
	pool := new(identifierRoutinePool)
	pool.name = name
	pool.handler = handler
	pool.addIdentifyRoutines(identifiers)
	return pool
}

func (self *identifierRoutinePool) addIdentifyRoutines(identifiers []string) {
	self.routineNums += len(identifiers)
	for _, idenfier := range identifiers {
		w := new(worker)
		w.identifier = idenfier
		w.handler = self.handler
		w.batchChan = make(chan batch)
		w.pending = 0
		w.isForced = false
		self.workers = append(self.workers, w)
		go w.work()
	}
}

// remove the identier routine
func (self *identifierRoutinePool) removeRoutine(identifier string) {
	log.Info("remove %s routine", identifier)
	self.Lock()
	defer self.Unlock()
	self.routineNums--
	for i, w := range self.workers {
		if w.identifier == identifier {
			w.isForced = true
			self.workers = append(self.workers[:i], self.workers[i+1:]...)
			return
		}
	}
}
