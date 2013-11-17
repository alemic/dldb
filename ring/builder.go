package ring

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/senarukana/dldb/log"
	"github.com/senarukana/dldb/zk"
	"math"
	"math/rand"
	"sort"
	"time"
)

type RingMoves map[string][]int

var (
	defaultZkUrl  = "127.0.0.1:2184"
	defaultZkPort = 2184
	ringBasePath  = "/dldb/ring"
	ringPath      = "/dldb/ring/ring"

	proxyPath = "/dldb/ring/proxy"
	proxyLock = "/dldb/ring/proxy/lock"

	dataPath = "/dldb/ring/data"
	dataLock = "/dldb/ring/data/lock"
)

/*// t is true means proxy, false means data
func makePartitionPath(t bool, partition int) string {
	path := fmt.sp
}*/

// Used to build ring instances to be written to disk

/*
	part_power: number of partitions = 2**part_power
    param replicas: number of replicas for each partition
    param min_part_hours: minimum number of hours between partition changes
*/
type RingBuilder struct {
	partitions         int
	partPower          int
	replicas           int
	minPartHours       int64
	devs               devices
	removeDevs         devices
	replica2part2devid [][]int
	ring               *Ring
	lastPartMoves      map[int]int64
	lastMoveEpoch      int64
	isChanged          bool
	versions           int64
}

func InitRingBuilder(partPower int, replicas int, minPartHours int64) *RingBuilder {
	builder := new(RingBuilder)
	builder.partPower = partPower
	builder.partitions = 1 << uint32(partPower)
	builder.replicas = replicas
	builder.minPartHours = minPartHours
	builder.replica2part2devid = make([][]int, replicas)
	builder.lastPartMoves = make(map[int]int64)
	for i := 0; i < replicas; i++ {
		builder.replica2part2devid[i] = make([]int, builder.partitions)
	}
	return builder
}

/*  Note: this will not rebalance the ring immediately as you may want to
make multiple changes for a single rebalance
*/
func (self *RingBuilder) AddDev(d *deviceInfo, forced bool) {
	dev := new(device)
	dev.host = d.host
	dev.weight = d.weight
	dev.zone = d.zone
	dev.description = d.description
	dev.id = len(self.devs)
	if len(self.devs) == 0 {
		self.devs = append(self.devs, dev)
	} else {
		/*		if dev.id != 0 && dev.id < len(self.devs) && self.devs[dev.id] != nil {
				panic(fmt.Sprintf("[Ring]the device %s is already existed", dev.host))
			}*/
		// check if the device is aldready existed
		for _, d := range self.devs {
			if d.host == dev.host {
				panic(fmt.Sprintf("[Ring]the device %s is already existed", d.host))
			}
		}
		self.devs = append(self.devs, dev)
	}
	self.isChanged = true
	self.versions++
	if forced {
		self.Rebalance()
	}
}

func (self *RingBuilder) RemoveDev(host string) bool {
	for _, dev := range self.devs {
		if dev.host == host {
			dev.weight = 0
			self.removeDevs = append(self.removeDevs, dev)
			self.versions++
			self.isChanged = true
			return true
		}
	}
	return false
}

func (self *RingBuilder) SetDevWeight(host string, weight int) {
	for _, dev := range self.devs {
		if dev.host == host {
			dev.weight = weight
			self.versions++
			self.isChanged = true
		}
	}
}

// Sets the parts_wanted key for each of the devices to the number of
// partitions the device wants based on its relative weight
func (self *RingBuilder) setDevPartsWanted() {
	totalWeight := 0
	for _, dev := range self.devs {
		totalWeight += dev.weight
	}
	weightedParts := int(self.partitions / totalWeight)
	for _, dev := range self.devs {
		dev.partsWanted = dev.weight*weightedParts - dev.parts
		dev.sort_key = fmt.Sprintf("%08d.%04d", dev.partsWanted, rand.Int())
	}
	/*	if len(self.devs) == 3 {
		panic(fmt.Sprintln(self.devs[0].partsWanted, self.devs[1].partsWanted, self.devs[2].partsWanted))
	}*/
}

func (self *RingBuilder) Rebalance() (bool, float64) {
	if !self.isChanged {
		return false, 0
	}
	self.setDevPartsWanted()
	if self.lastMoveEpoch == 0 {
		self.initRing()
		self.isChanged = false
		return true, self.getBalance()
	}
	self.updateLastPartMoves()
	var last_balance float64 = 0
	maxLoop := 10
	i := 0
	for i < maxLoop {
		reassigns := self.gatherReassignParts()
		self.reassignParts(reassigns)
		balance := self.getBalance()
		if balance < 1 || math.Abs(last_balance-balance) < 1 {
			break
		}
		last_balance = balance
		i++
	}
	self.isChanged = false
	self.versions++
	return true, last_balance

}

/*
 * Once rebalance operation has been done, we need to register the new data placement information to the zookeeper
 * It will build 2 rings, one for Proxy and another for Data server. The structure of two rings are the same, but data may not.

   Example:

    ring -----+----proxy----+----  patition1 ---------+-----device id 0
              |             |                         |
              |             |                         +---- device id 1
              |             |                         |
              |             |                         +---- device id 2
              |             |
              |             +---- partition2 ------- -+-----device id 3
              |             |                         |
              |             |                         +---- device id 4
              |             |                         |
              |             |                         +---- device id 5
              |				|
              |             +---- partition3 ---------+-----device id 2
              |                                       |
              |                                       +---- device id 3
              |                                       |
              |                                       +---- device id 5
              |
              +----data-----+------patition1 ---------+-----device id 3
              |             |                         |
              |             |                         +---- device id 1
              |             |                         |
              |             |                         +---- device id 2
              |             |
              |             +---- partition2 ---------+-----device id 1
              |             |                         |
              |             |                         +---- device id 4
              |             |                         |
              |             |                         +---- device id 5
              |				|
              |             +---- partition3 ------- +------device id 4
              |                                       |
              |                                       +---- device id 3
              |                                       |
              |                                       +---- device id 5

 * Proxy server use the ring to route the requests.
 * Data server use the ring to check if there are some changes to the data distribution and it has to migrate data
 * two rings may not exactly the same. Because once a reblance operation has been issued, it will not modify the proxy ring quickly,
 * until data migration has been done.
 * But rebalance operation will make data ring change immediately.After doing that, the data server will quickly detect this, and do the migration job.
 * Once the migration has been done, it then modifies the proxy ring to inform the proxy, and finally change the routine rules.
 * Build steps are :
 * 1. Get a exclusive lock for  to forbiden others do the same thing
 * 2. Iterate the paritions according to replica2part2devid, check if the partition placement has been changed.
 * 3. Modify the partition information
*/
func (self *RingBuilder) BuildRingToZooKeeper() error {
	// get ZK Connection
	zkConn, _, err := zk.GetConn(defaultZkUrl)
	if err != nil {
		return err
	}
	//step 1
	if _, err := zkConn.RecursiveTouch(dataLock); err != nil {
		return err
	}
	m := zk.NewMutex(zkConn.Conn, dataLock)
	getted, err := m.TryLock()
	if err != nil {
		return err
	}
	if !getted {
		return errors.New("Oops, can't get the data lock, someone is doing the same thing? give up")
	}
	defer m.Unlock()
	// step2
	for partition := 0; partition < self.partitions; partition++ {
		// get original partition info
		dataPartitionPath := fmt.Sprintf("%s/%d", dataPath, partition)
		children, _, err := zkConn.Children(dataPartitionPath)
		if err != nil {
			return err
		}
		var newChildren []string
		for replica := 0; replica < self.replicas; replica++ {
			newChildren = append(newChildren, self.devs[self.replica2part2devid[replica][partition]].host)
		}
		// step3
		if !stringsCompare(children, newChildren) {
			// not the same, rewrite the partition
			// delete existed partition info
			zkConn.RecursiveDelete(dataPartitionPath)
			// create new partition info
			for _, host := range newChildren {
				if _, err := zkConn.Touch(fmt.Sprintf("%s/%d", dataPartitionPath, host)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (self *RingBuilder) InitRingToZooKeeper() error {
	log.Info("Begin init ring to zookeeper")
	// get ZK Connection
	zkConn, _, err := zk.GetConn(defaultZkUrl)
	if err != nil {
		return err
	}
	//step 1
	if _, err := zkConn.RecursiveTouch(dataLock); err != nil {
		return err
	}
	dataMutex := zk.NewMutex(zkConn.Conn, dataLock)
	getted, err := dataMutex.TryLock()
	if err != nil {
		return err
	}
	if !getted {
		return errors.New("Oops, can't get the data lock, someone is doing the same thing? give up")
	}
	defer dataMutex.Unlock()
	if _, err := zkConn.RecursiveTouch(proxyLock); err != nil {
		return err
	}
	proxyMutex := zk.NewMutex(zkConn.Conn, proxyLock)
	getted, err = proxyMutex.TryLock()
	if err != nil {
		return err
	}
	if !getted {
		return errors.New("Oops, can't get the proxy lock, someone is doing the same thing? give up")
	}
	defer proxyMutex.Unlock()
	// step2
	for partition := 0; partition < self.partitions; partition++ {
		// get original partition info
		dataPartitionPath := fmt.Sprintf("%s/%d", dataPath, partition)
		if _, err := zkConn.Touch(dataPartitionPath); err != nil {
			return err
		}
		proxyPartitionPath := fmt.Sprintf("%s/%d", proxyPath, partition)
		if _, err := zkConn.Touch(proxyPartitionPath); err != nil {
			return err
		}
		for replica := 0; replica < self.replicas; replica++ {
			devId := self.replica2part2devid[replica][partition]
			host := self.devs[devId].host
			// create new partition info
			if _, err := zkConn.Touch(fmt.Sprintf("%s/%s", dataPartitionPath, host)); err != nil {
				return err
			}
			if _, err := zkConn.Touch(fmt.Sprintf("%s/%s", proxyPartitionPath, host)); err != nil {
				return err
			}
		}
	}
	log.Info("init ring to zk Complete")
	return nil
}

// push the entire ring to zookeeper. for those who are new in the cluster
func (self *RingBuilder) PushRingDataToZooKeeper() error {
	// get ZK Connection
	zkConn, _, err := zk.GetConn(defaultZkUrl)
	if err != nil {
		return err
	}
	ring := InitRingFromBuilder(self.devs, self.replica2part2devid, 32-self.partPower, self.versions)
	if _, err := zkConn.RecursiveTouch(ringBasePath); err != nil {
		return err
	}
	// gob encoding
	buffer := new(bytes.Buffer)
	enc := gob.NewEncoder(buffer)
	err = enc.Encode(ring)
	if err != nil {
		return err
	}
	_, err = zkConn.CreatePersistenceNode(ringPath, string(buffer.Bytes()))
	if err != nil {
		return err
	}
	return nil
}

func (self *RingBuilder) updateLastPartMoves() {
	// elapsedHours := int64((time.Now().Unix() - self.lastMoveEpoch) / 3600)
	var elapsedHours int64 = 10
	for part := 0; part < self.partitions; part++ {
		self.lastPartMoves[part] = self.lastPartMoves[part] + elapsedHours
	}
	self.lastMoveEpoch = time.Now().Unix()
}

func (self *RingBuilder) gatherReassignParts() (reassignParts []int) {
	start := rand.Int() % self.partitions
	for replica := 0; replica < self.replicas; replica++ {
		part2devId := self.replica2part2devid[replica]
		//TODO
		for part := start; part < self.partitions; part++ {
			if self.lastPartMoves[part] >= self.minPartHours {
				dev := self.devs[part2devId[part]]
				if dev.partsWanted < 0 {
					part2devId[part] = -1
					self.lastPartMoves[part] = 0
					dev.partsWanted++
					dev.parts--
					reassignParts = append(reassignParts, part)
				}
			}
		}
		for part := 0; part < start; part++ {
			if self.lastPartMoves[part] >= self.minPartHours {
				dev := self.devs[part2devId[part]]
				if dev.partsWanted < 0 {
					part2devId[part] = -1
					self.lastPartMoves[part] = 0
					dev.partsWanted++
					dev.parts--
					reassignParts = append(reassignParts, part)
				}
			}
		}
	}
	/*	if len(self.devs) == 3 {
		panic(fmt.Sprintln(reassignParts))
	}*/
	return reassignParts
}

func (self *RingBuilder) reassignParts(parts []int) {
	for _, dev := range self.devs {
		dev.sort_key = fmt.Sprintf("%08d.%04d", dev.partsWanted, rand.Int())
	}
	// sort by sort_key
	sort.Sort(self.devs)
	for _, parition := range parts {
		// make sure the replica is in different zone
		other_zones := make(map[string]bool)
		for replica := 0; replica < self.replicas; replica++ {
			i := 0
			for ; i < self.devs.Len(); i++ {
				// 0 means removed dev
				if self.devs[i].weight != 0 && !other_zones[self.devs[i].zone] {
					break // not in the same zone
				}
			}
			// there are no different zones, just choose the first one
			if i == self.devs.Len() {
				i = 0
			}
			dev := self.devs[i]
			self.replica2part2devid[replica][parition] = dev.id
			dev.partsWanted--
			dev.parts++
			dev.sort_key = fmt.Sprintf("%08d.%04d", dev.partsWanted, rand.Int())
			self.devs.Order(dev, i+1)
			other_zones[dev.zone] = true
		}
	}
	/*	if len(self.devs) == 3 {
		panic(fmt.Sprintln(self.devs[0].id, self.devs[1].id, self.devs[2].id))
	}*/
}

func (self *RingBuilder) initRing() {
	// sort by sort_key
	sort.Sort(self.devs)
	for parition := 0; parition < self.partitions; parition++ {
		// make sure the replica is in different zone
		other_zones := make(map[string]bool)
		for replica := 0; replica < self.replicas; replica++ {
			i := 0
			for ; i < self.devs.Len(); i++ {
				// 0 means removed dev
				if self.devs[i].weight != 0 && !other_zones[self.devs[i].zone] {
					break // not in the same zone
				}
			}
			// there are no different zones, just choose the first one
			if i == self.devs.Len() {
				i = 0
			}
			dev := self.devs[i]
			self.replica2part2devid[replica][parition] = dev.id
			dev.partsWanted--
			dev.parts++
			dev.sort_key = fmt.Sprintf("%08d.%04d", dev.partsWanted, rand.Int())
			self.devs.Order(dev, i+1)
			other_zones[dev.zone] = true
		}
	}
	self.lastMoveEpoch = time.Now().Unix()
}

// Get the balance of the ring. The balance value is
// the highest percentage off the desired desired amount of partitions a given device wants

func (self *RingBuilder) getBalance() float64 {
	totalWeight := 0
	for _, dev := range self.devs {
		totalWeight += dev.weight
	}
	weightedParts := int(self.partitions / totalWeight)
	var balance float64 = 0
	for _, dev := range self.devs {
		dev_balance := math.Abs(100.0*float64(dev.parts)/float64(dev.weight*weightedParts) - 100.0)
		if dev_balance > balance {
			balance = dev_balance
		}
	}
	return balance
}
