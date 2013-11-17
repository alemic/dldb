package balancer

import (
	"fmt"
	"github.com/senarukana/dldb/conf"
	"github.com/senarukana/dldb/log"
	"time"
)

/**
 * Manager that uses the workAggregator mechanism to
 * automatically determine the batch size.
 */
type workAggregator interface {
	GetBlockTime() time.Duration
	GetAggregatorTarget() int
}

type aggregatorType func(balancer *Balancer, conf *conf.WorkAggregatorConfiguration) workAggregator

var aggregatorAdapters = make(map[string]aggregatorType)

func RegisterAggregator(name string, aggregator aggregatorType) {
	if aggregator == nil {
		panic("aggregator: registered aggregator is nil")
	}
	if _, dup := aggregatorAdapters[name]; dup {
		panic("aggregator: register aggregator called twice")
	}
	aggregatorAdapters[name] = aggregator
	log.Info("aggregator %s init complete", name)
}

type workAggregatorManager struct {
	balancer   *Balancer
	aggregator workAggregator
}

func initAggregatorManager(aggregatorName string, blancer *Balancer, configure *conf.WorkAggregatorConfiguration) *workAggregatorManager {
	aggregatorManager := new(workAggregatorManager)
	aggregatorManager.balancer = blancer
	if aggregator, ok := aggregatorAdapters[aggregatorName]; ok {
		aggregatorManager.aggregator = aggregator(blancer, configure)
		return aggregatorManager
	} else {
		panic(fmt.Sprintf("aggregator: Aggregator %s is not existed", aggregatorName))
	}
}

func (self *workAggregatorManager) getBatch() batch {
	aggregateTarget := self.aggregator.GetAggregatorTarget()
	// log.Trace("[%s] aggregate target is %d", self.name, aggregateTarget)
	requests := self.balancer.requestQueue.dequeue(aggregateTarget, self.aggregator.GetBlockTime())
	// log.Trace("[%s] get batch request num is %d", self.name, len(requests))
	if len(requests) == 0 {
		return nil
	}
	bat := initWorkBatch(self.balancer.poolManager.doneChan, requests)
	return bat
}
