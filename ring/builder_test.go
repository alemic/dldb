package ring

import (
	"github.com/senarukana/dldb/zk"
	"testing"
)

func TestBuilder(t *testing.T) {
	builder := InitRingBuilder(4, 1, 1)
	dev1 := new(deviceInfo)
	dev1.host = "127.0.0.1:1010"
	dev1.weight = 1
	dev1.zone = "z1"
	dev1.description = "first"
	builder.AddDev(dev1, false)
	dev2 := new(deviceInfo)
	dev2.host = "127.0.0.2:1010"
	dev2.weight = 1
	dev2.zone = "z1"
	dev2.description = "second"
	builder.AddDev(dev2, false)
	builder.Rebalance()
	if builder.devs[0].parts != builder.devs[1].parts {
		t.Errorf("dev parts are not the same dev0 parts")
	}

	dev3 := new(deviceInfo)
	dev3.host = "127.0.0.3:1010"
	dev3.weight = 2
	dev3.zone = "z1"
	dev3.description = "third"
	builder.AddDev(dev3, false)
	builder.Rebalance()
	// builder.Rebalance()
	if builder.devs[0].parts == builder.devs[2].parts {
		t.Fatalf("parts of dev1 and dev3 shouldn't be the same")
	}
	dev4 := new(deviceInfo)
	dev4.host = "127.0.0.4:1010"
	dev4.weight = 2
	dev4.zone = "z1"
	dev4.description = "forth"
	builder.AddDev(dev4, false)
	builder.Rebalance()
	/*	if builder.devs[3].parts != builder.devs[2].parts {
		t.Fatalf("parts of dev3 and dev4 should be the same, parts3 %d, parts4 %d",
			builder.devs[2].parts, builder.devs[3].parts)
	}*/
	testRingToZK(builder, t)

}

func testRingToZK(builder *RingBuilder, t *testing.T) {
	// test init
	server := zk.NewZkServer(defaultZkPort, "/tmp/zktest", "/home/ted/zookeeper/zookeeper-3.4.5")
	defer server.Destroy()
	if err := server.Init(); err != nil {
		t.Fatalf("couldn't set up zk server: %v", err)
	}
	t.Log("set up successfully")

	err := builder.InitRingToZooKeeper()
	if err != nil {
		t.Error(err)
	}

	conn, _, err := zk.GetConn(defaultZkUrl)
	if err != nil {
		t.Errorf("Should get the connection:%v", err)
	}
	children, _, err := conn.Children(proxyPath)
	if err != nil {
		t.Errorf("get children error %v", err)
	}
	// one for lock
	if len(children) != builder.partitions+1 {
		t.Error("partitions should be equal to the num of data proxy path's node, children %d, paritions %d",
			len(children), builder.partitions)
	}

	// test push ring to zk
	if err := builder.PushRingDataToZooKeeper(); err != nil {
		t.Error("push ring to zk error")
	}
	ring, err := initRingDataFromZooKeeper()
	if err != nil {
		t.Error("read ring from zk error : ", err)
	}
	if len(ring.part2repli2devHost) != builder.partitions || len(ring.part2repli2devHost[0]) != builder.replicas {
		t.Error("ring recover from zk error")
	}

}
