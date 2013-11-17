package balancer

/*import (
	"github.com/senarukana/dldb/conf"
)

// The ThreadPoolController is responsible for dynamically adusting the size of
// a given ThreadPool.
const (
	defaultMoniterInterval = 5 * time.Millisecond
	defaultThreshould      = 100
	// Multiple of standard controller delay
	controllerDelay            = 4
	throughputMeasurementDelay = 1
	autoMaxDetectDelay         = 10
)

type routinePoolController struct {
	routinePools []routinePool

	// configuration for adjust routine num
	threshould int  // if requests exceed the threshould, we will create a new routine to handle
	autoDetect bool // whether or not change the routine pool dynamically according to the throughput
	// parameteres for record the the throughput and routines
	// this is inspired by tcp
	savedRoutines   int // routine num at the best throughput moment
	avgRoutines     int
	savedThroughput float32
	avgThroughput   float32

	adjustCount int64 // counter for moniter
}

func initRoutinePoolController(globalManager *GlobalBalancerManager, configure *conf.ConfigFile) {
	routinePoolController
	if moniterInterval, err := globalManager.configure.GetString(balancerSectionName, "montiterInterval"); err == nil {
		manager.interval = moniterInterval
	} else {
		manager.interval = defaultThreshould
	}

	if threshould, err := globalManager.configure.GetString(balancerSectionName, "threshould"); err == nil {
		manager.threshould = threshould
	} else {
		manager.interval = defaultThreshould
	}
}

func (self *routinePoolController) adjustThreadPools() {
	self.adjustCount++

	// if requests exceed the threshould, we will create a new routine to handle
	if self.adjustCount%controllerDelay == 0 { // check the requests
		if DEBUG {
			fmt.Fprintf("stage %s, request num: %d", self.name, self.balancer.requests)
		}
		if self.balancer.requests > self.threshould {
			self.pool.addRoutines(1)
		}
	}

	if !self.autoDetect {
		return
	}

	//  change the routine pool dynamically according to the throughput
	// we should first record the throughput
	if self.adjustCount%throughputMeasurementDelay == 0 {

	}
}
*/
