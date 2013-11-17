package balancer

import (
	"github.com/senarukana/dldb/conf"
	"time"
)

/*
 * AggThrottle is used by thread managers to adjust their aggregation level
 * based on observations of stage throughput.
 */

const (
	reduce_facor    = 1.2
	increase_factor = 1.2
	low_water       = 0.80
	high_water      = 1.02
	very_low_water  = 0.2
	very_high_water = 2.0
	adjust_delay    = 5
)

type simpleAggregator struct {
	balancer        *Balancer
	blockTime       time.Duration
	aggregateLimits int
	minAggregation  int
	maxAggregation  int
	recalcWindow    int64
	smoothConst     float64

	// used to measure the throughput
	bestThroughput          float64
	avgThroughput           float64
	bestAggregationTarget   int
	savedAggreagationTarget int
	savedRequests           int
	savedMeasureTime        int64
	adjuestCount            int64
	state                   bool //true means increasing false means decreasing
}

func (self *simpleAggregator) Init(balancer *Balancer, conf *conf.WorkAggregatorConfiguration) {
	self.balancer = balancer
	self.aggregateLimits = conf.AggregateLimits
	self.blockTime = conf.BlockTime
	self.minAggregation = conf.MinAggreagation
	self.maxAggregation = conf.MaxAggreagation
	self.recalcWindow = conf.RecalcWindow
	self.smoothConst = conf.SmoothConst
	self.state = false // increase direction
	self.savedAggreagationTarget = self.minAggregation
}

func (self *simpleAggregator) GetBlockTime() time.Duration {
	return self.blockTime
}

// return the aggregation level
func (self *simpleAggregator) GetAggregatorTarget() int {
	// if the system is not busy, no aggregate
	if self.balancer.requests < self.aggregateLimits {
		return 1
	}
	self.adjuestCount++
	// milliseconds time we begin measure
	curTime := time.Now().UnixNano() % 1e6 / 1e3
	timeElapsed := curTime - self.savedMeasureTime
	if timeElapsed < self.recalcWindow {
		return self.savedAggreagationTarget
	}
	completedRequests := self.balancer.completeRequests - self.savedRequests
	self.savedRequests = completedRequests
	self.savedMeasureTime = curTime

	// calculate throughput
	throughput := float64(completedRequests) / float64((curTime-self.savedMeasureTime)*1.0e3)
	self.avgThroughput = self.smoothConst*float64(throughput) + (1.0-self.smoothConst)*self.avgThroughput
	if self.adjuestCount%adjust_delay == 0 {
		//throughput is too low, need to increase batch target
		/*		if self.avgThroughput < very_low_water*self.bestThroughput {
					self.state = true
				} else if self.avgThroughput > very_high_water*self.bestThroughput { //thourghput is too high
					self.state = false
				}*/
		// increase batch target
		if self.state {
			if self.avgThroughput > self.bestThroughput {
				self.bestThroughput = self.avgThroughput
			}
			// the current throughput is high enough, decrease the batch target
			if self.avgThroughput < high_water*self.bestThroughput {
				self.state = false
				self.savedAggreagationTarget = int(float64(self.savedAggreagationTarget) / reduce_facor)
				if self.savedAggreagationTarget < self.minAggregation {
					self.savedAggreagationTarget = self.minAggregation
				}
			} else {
				//just increase the batch target
				self.savedAggreagationTarget = int(float64(self.savedAggreagationTarget) * increase_factor)
				if self.savedAggreagationTarget > self.maxAggregation {
					// batch target is too high, decrease it
					self.savedAggreagationTarget = self.maxAggregation
					self.bestThroughput = self.avgThroughput
					self.state = false
				}
			}
		} else {
			if self.avgThroughput > self.bestThroughput {
				self.bestThroughput = self.avgThroughput
			}
			// decrease batch target
			// reduce the batch target lead to a sharp decrease then increate the batch target
			if self.avgThroughput < low_water*self.bestThroughput {
				self.state = true
				self.savedAggreagationTarget = int(float64(self.savedAggreagationTarget) * increase_factor)
			} else {
				//just decrease the batch target
				self.savedAggreagationTarget = int(float64(self.savedAggreagationTarget) / reduce_facor)
				if self.savedAggreagationTarget < self.minAggregation {
					// batch target is too low, increase it
					self.savedAggreagationTarget = self.minAggregation
					self.state = true
				}
			}
		}

		//randomly reset the direction
	}
	return self.savedAggreagationTarget
}

func newSimpleAggregator(balancer *Balancer, conf *conf.WorkAggregatorConfiguration) workAggregator {
	agg := new(simpleAggregator)
	agg.Init(balancer, conf)
	return agg
}

func init() {
	RegisterAggregator("simple", newSimpleAggregator)
}
