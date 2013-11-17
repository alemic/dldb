package balancer

/*import (
	"fmt"
	"github.com/senarukana/dldb/conf"
)

type GlobalBalancerManager struct {
	configure *conf.ConfigFile
	balancers map[string]*Balancer
}

func (self *GlobalBalancerManager) registerBalancer(name string, handler EventHandler) *Balancer {
	b := NewBalancer(name, handler, self)
	self.balancers[name] = b
	return b
}

func NewGlobalBalancer(configure *conf.ConfigFile) *GlobalBalancerManager {
	balancerManager := new(GlobalBalancerManager)
	balancerManager.configure = configure
	balancerManager.balancers = make(map[string]*Balancer)
	return balancerManager
}

func (self *GlobalBalancerManager) GetBalancerChannel(name string) (chan *Request, error) {
	if balancer, ok := self.balancers[name]; ok {
		return balancer.requestChan, nil
	} else {
		panic(fmt.Sprintf("balancer %s is not existed, forget to register it?", name))
	}
}
*/
