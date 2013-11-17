package balancer

type workerPool []*worker

//-------------- method for the heap-----------------
func (p workerPool) Less(i, j int) bool {
	return p[i].pending < p[j].pending
}

func (p workerPool) Len() int {
	return len(p)
}

func (p *workerPool) Push(i interface{}) {
	n := len(*p)
	item := i.(*worker)
	item.index = n
	*p = append(*p, item)
}

func (p *workerPool) Pop() interface{} {
	a := *p
	n := len(a)
	item := a[n-1]
	*p = a[0 : n-1]
	return item
}

func (p *workerPool) Swap(i, j int) {
	t := (*p)[i]
	(*p)[i] = (*p)[j]
	(*p)[j] = t
}
