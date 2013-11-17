package zk

import (
	"launchpad.net/gozk/zookeeper"
	"os"
)

type ZkServer struct {
	Server      *zookeeper.Server
	Zk          *ZkConn
	ZkEventChan <-chan zookeeper.Event
	Port        int
	RunDir      string
	zkPath      string
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
func NewZkServer(port int, runDir string, zkPath string) *ZkServer {
	return &ZkServer{Port: port, RunDir: runDir, zkPath: zkPath}
}

func (self *ZkServer) Init() error {
	os.RemoveAll(self.RunDir)
	var err error
	_, err = exists(self.zkPath)
	if err != nil {
		return err
	}
	if self.Server, err = zookeeper.CreateServer(self.Port, self.RunDir, self.zkPath); err != nil {
		return err
	}
	if err = self.Server.Start(); err != nil {
		return err
	}
	addr, err := self.Server.Addr()
	if err != nil {
		return err
	}
	self.Zk, self.ZkEventChan, err = GetConn(addr)
	if err != nil {
		return err
	}
	go waitOnConnect(self.ZkEventChan)
	return nil
}

func (self *ZkServer) Destroy() error {
	self.Zk.Conn.Close()
	err := self.Server.Destroy()
	if err != nil {
		return err
	}
	os.RemoveAll(self.RunDir)
	return nil
}
