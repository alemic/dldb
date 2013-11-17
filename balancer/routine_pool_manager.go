package balancer

import (
	"container/heap"
	"github.com/senarukana/dldb/conf"
	"time"
)

type routinePoolManager struct {
	name         string
	balancerType string
	// only have one pool for manager
	pool          *routinePool
	identiferPool *identifierRoutinePool

	balancer *Balancer
	doneChan chan *job //job complete

	// configuration for adjust routine num
	monitorInterval time.Duration
	threshould      int  // if requests exceed the threshould, we will create a new routine to handle
	autoDetect      bool // whether or not change the routine pool dynamically according to the throughput
	// parameteres for record the the throughput and routines
	// this is inspired by tcp
	savedRequests    int
	savedRoutines    int // routine num at the best throughput moment
	savedMeasureTime int64
	avgRoutines      int
	savedThroughput  float64
	avgThroughput    float64

	adjustCount int64 // counter for moniter
}

func initRoutinePoolManager(name string, balancerType string, balancer *Balancer, handler EventHandler,
	configuration *conf.BalancerConfiguration) (manager *routinePoolManager) {
	manager = new(routinePoolManager)
	manager.name = name
	manager.balancer = balancer
	manager.balancerType = balancerType
	manager.doneChan = make(chan *job)
	if balancerType == "identifier" {
		manager.identiferPool = initIdentifierRoutinePool(name, handler, configuration.Identifiers)
	} else {
		manager.pool = initRoutinePool(name, configuration.InitRoutineNum, handler, configuration.RoutineIdleTime, configuration.MinRoutineNum)
	}
	manager.threshould = configuration.RequestsThreshould
	manager.monitorInterval = configuration.MoniterInterval
	manager.autoDetect = configuration.ManagerAutoDetect
	return manager
}

func (self *routinePoolManager) moniter() {
	if self.balancerType != "identifier" {
		for {
			c := time.After(self.monitorInterval)
			select {
			case j := <-self.doneChan:
				self.jobComplete(j) // job complete
			case _ = <-c:
				// begin moniter
				self.adjustThreadPools()
			}
		}
	} else {
		for j := range self.doneChan {
			self.jobComplete(j)
		}
	}
}

// Job is completed
func (self *routinePoolManager) jobComplete(j *job) {
	self.balancer.completeRequests += j.requestNum
	// decrease the requests
	self.balancer.requests -= j.requestNum
	// one fewer in the queue
	j.w.pending -= j.requestNum
	// identifer pool don't need to sort
	if self.balancerType != "identifier" {
		// Remove it from heap
		heap.Remove(&self.pool.workers, j.w.index)
		// Put it into its place on the heap
		heap.Push(&self.pool.workers, j.w)
	}
}

func (self *routinePoolManager) adjustThreadPools() {
	self.adjustCount++

	// if requests exceed the threshould, we will create a new routine to handle
	if self.adjustCount%controllerDelay == 0 { // check the requests
		if self.balancer.requests > self.threshould {
			self.pool.addRoutines(1)
			return
		}
	}

	if !self.autoDetect {
		return
	}

	//  change the routine pool dynamically according to the throughput
	// we should first record the throughput
	/*if self.adjustCount%throughputMeasurementDelay == 0 {
		// milliseconds time we begin measure
		curTime := time.Now().UnixNano() % 1e6 / 1e3
		// completed requests since the last time
		completedRequest := self.balancer.completeRequests - self.savedRequests
		curRoutines := self.pool.routineNums

		self.avgRoutines := int(smoothConst * curRoutines + (1.0 - smoothConst) * self.avgRoutines)
		throughput := float64(completedRequest) / float64((curTime - self.savedMeasureTime) * 1.0e3)
		self.avgThroughput := smoothConst * float64(throughput) + (1.0 - smoothConst) * self.avgThroughput

		self.savedMeasureTime = curTime
	}

	//
	if self.adjustCount %autoMaxDetectDelay ==0 {
		// current throughput is superior to the history highest throughput, we should record it
		if self.avgThroughput > self.savedThroughput {
			self.savedThroughput = self.avgThroughput
			self.savedRoutines = self.pool.routineNums //save the current routine nums
		} else if self.avgThroughput < 1.2 * self.savedThroughput &&  { // the current throughput i

		}
	} */

}
