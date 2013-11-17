package ring

import (
	"bytes"
	"encoding/gob"
	"github.com/senarukana/dldb/log"
	"github.com/senarukana/dldb/zk"
)

type Ring struct {
	part2repli2devHost [][]string
	hashShift          int
	versions           int64
	partitions         int
	replicas           int
}

func (self *Ring) GobEncode() ([]byte, error) {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(self.part2repli2devHost)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(self.hashShift)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(self.versions)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (self *Ring) GobDecode(buf []byte) error {
	log.Trace("buf len is %d", len(buf))
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	err := decoder.Decode(&self.part2repli2devHost)
	if err != nil {
		return err
	}
	err = decoder.Decode(&self.hashShift)
	if err != nil {
		return err
	}
	return decoder.Decode(&self.versions)
}

func InitRingFromBuilder(devs devices, replica2part2devid [][]int, hashShift int, versions int64) *Ring {
	ring := new(Ring)
	ring.replicas = len(replica2part2devid)
	ring.partitions = len(replica2part2devid[0])
	ring.part2repli2devHost = make([][]string, ring.partitions)
	for partition := 0; partition < ring.partitions; partition++ {
		ring.part2repli2devHost[partition] = make([]string, ring.replicas)
		for replica := 0; replica < ring.replicas; replica++ {
			ring.part2repli2devHost[partition][replica] = devs[replica2part2devid[replica][partition]].host
		}
	}
	ring.hashShift = hashShift
	ring.versions = versions
	return ring
}

// Init ring info from zk.
// When proxy and data servers initiate, they will reads this.
func InitRingDataFromZooKeeper() (*Ring, error) {
	zkConn, _, err := zk.GetConn(defaultZkUrl)
	if err != nil {
		return nil, err
	}
	ringString, _, err := zkConn.Get(ringPath)
	if err != nil {
		return nil, err
	}
	log.Trace("ringString length is %d", len(ringString))
	ring := new(Ring)
	ringByte := []byte(ringString)
	log.Trace("ringbyte length is %d", len(ringByte))
	buffer := bytes.NewBuffer([]byte(ringString))

	dec := gob.NewDecoder(buffer)
	err = dec.Decode(ring)
	if err != nil {
		return nil, err
	}
	ring.replicas = len(ring.part2repli2devHost[0])
	ring.partitions = len(ring.part2repli2devHost)
	return ring, nil
}

func (self *Ring) GetPartitions() int {
	return self.partitions
}

func (self *Ring) GetReplicas() int {
	return self.replicas
}

/*func (self *Ring) MontierRing()  ([]chan zookeeper.Event, error) {
	self.
}*/

// return nodes that owns the partition
func (self *Ring) GetPartNodes(partition int) ([]string, error) {
	if partition > len(self.part2repli2devHost) {
		return nil, errInvalidPartition
	}
	return self.part2repli2devHost[partition], nil
}
