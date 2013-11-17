package balancer

import (
	"github.com/senarukana/dldb/conf"
	"time"
)

// this is for the balancer that don't need aggregate operations
type noneAggregator struct {
}

func (self *noneAggregator) Init(balancer *Balancer) {}

func (self *noneAggregator) GetBlockTime() time.Duration {
	return time.Minute
}
func (self *noneAggregator) GetAggregatorTarget() int {
	return 1
}

func newNoneAggregator(balancer *Balancer, conf *conf.WorkAggregatorConfiguration) workAggregator {
	agg := new(noneAggregator)
	return agg
}

func init() {
	RegisterAggregator("none", newNoneAggregator)
}
