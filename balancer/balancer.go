package balancer

import (
	"container/heap"
	"errors"
	"github.com/senarukana/dldb/conf"
	"github.com/senarukana/dldb/log"
)

const (
	DEBUG = true
	// Multiple of standard controller delay
	controllerDelay            = 4
	throughputMeasurementDelay = 1
	autoMaxDetectDelay         = 10
	smoothConst                = 0.3
)

/*type TooManyRequestsError string

func (self *TooManyRequestsError) Error() string {
	return "Sorry, there are too many requests"
}*/

var errTooManyRequests = errors.New("Sorry, there are too many requests. Please try again later")

type Request interface{}

type IdentifierRequest struct {
	Id string
	Request
}

type job struct {
	requestNum int
	w          *worker // worker to do the job
}

type EventHandler interface {
	EventHandle(item interface{})
	EventsHandle(items interface{})
	Init()
}

type BalancerHandler interface {
	Balance()
}

type Balancer struct {
	name              string
	balancerType      string
	handler           EventHandler
	poolManager       *routinePoolManager
	requestQueue      queue
	batchChan         chan batch
	aggregatorManager *workAggregatorManager
	requests          int //current requests
	completeRequests  int
	maxRequests       int64
}

type ProposeRequester interface {
	ProposeRequest(interface{}) error
}

// create a new balancer
func NewBalancer(name string, handler EventHandler, configure *conf.BalancersConfiguration) (balancer *Balancer) {

	balancer = new(Balancer)
	balancer.name = name
	balancer.handler = handler
	configuration := configure.GetBalancerConfiguration(name)
	balancer.balancerType = configuration.BalancerType
	balancer.initQueue(configuration)
	balancer.aggregatorManager = initAggregatorManager(configuration.AggreagatorName, balancer, configuration.WorkAggregatorConfiguration)
	balancer.poolManager = initRoutinePoolManager(name, balancer.balancerType, balancer, handler, configuration)
	return balancer
}

func (self *Balancer) initQueue(configuration *conf.BalancerConfiguration) {
	switch self.balancerType {
	case "priority":
		self.requestQueue = new(priorityQueue)
		self.requestQueue.init(self.name, []string{}, configuration.QueueBuffer, configuration.QueueLimits)
	case "identifier":
		self.requestQueue = new(identifierQueue)
		self.requestQueue.init(self.name, configuration.Identifiers, configuration.QueueBuffer, configuration.QueueLimits)
	default:
		panic("unknown queue name")
	}
}

func (self *Balancer) Init() {
	log.Info("init balancer")
	self.handler.Init()
	go self.poolManager.moniter()
	go self.balance()
}

// send request to this balancer
func (self *Balancer) ProposeRequest(request Request) error {
	log.Trace("[%s] balancer has received a request", self.name)
	// increment the requests
	self.requests++
	return self.requestQueue.enqueue(request)
}

// routine to schedule request
func (self *Balancer) balance() {
	for {
		batcher := self.aggregatorManager.getBatch() // received a request
		if batcher != nil {
			if self.balancerType == "identifier" {
				self.dispatchIdentifier(batcher)
			} else {
				self.dispatch(batcher)
			}
		}
	}
}

// Send Request to worker
func (self *Balancer) dispatch(batcher batch) {
	// TODO
	// Grab the least loaded worker
	w := heap.Pop(&self.poolManager.pool.workers).(*worker)
	// Dispatch the request to this worker
	w.batchChan <- batcher
	// One more in its worker queue
	w.pending += batcher.requestNum()
	// Put it into its place on the heap
	heap.Push(&self.poolManager.pool.workers, w)
}

// each identifier should only have 1 worker, so we need to dispatch to that one
func (self *Balancer) dispatchIdentifier(batcher batch) {
	// get the identifier
	request := batcher.get()[0]
	identifierRequest := request.(*IdentifierRequest)
	identifier := identifierRequest.Id
	// TODO
	var w *worker
	// Find the identifier worker
	for _, worker := range self.poolManager.pool.workers {
		if worker.identifier == identifier {
			w = worker
			break
		}
	}
	if w == nil {
		log.Error("[Balancer %s] can't find the worker", self.name)
		return
	}
	// Dispatch the request to this worker
	w.batchChan <- batcher
	// One more in its worker queue
	w.pending += batcher.requestNum()
}
