package balancer

type batch interface {
	get() []Request
	requestNum() int
	done(*worker)
}

type workBatch struct {
	requests []Request
	doneChan chan *job
}

func initWorkBatch(doneChan chan *job, requests []Request) *workBatch {
	batch := new(workBatch)
	batch.requests = requests
	batch.doneChan = doneChan
	return batch
}

func (self *workBatch) requestNum() int {
	return len(self.requests)
}

func (self *workBatch) get() []Request {
	return self.requests
}

func (self *workBatch) done(w *worker) {
	j := new(job)
	j.requestNum = len(self.requests)
	j.w = w
	self.doneChan <- j
}
