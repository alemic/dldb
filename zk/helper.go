package zk

import (
	"errors"
	"fmt"
	"github.com/senarukana/dldb/log"
	"launchpad.net/gozk/zookeeper"
	"path"
)

// Create a persistent node if it doesn't exist, otherwise do nothing
func (self *ZkConn) RecursiveTouch(nodePath string) (string, error) {
	if stat, err := self.Exists(nodePath); err == nil && stat != nil {
		return nodePath, nil
	}
	// touch all directories above nodePath
	dir := path.Dir(nodePath)
	if dir != "/" && dir != "." {
		self.RecursiveTouch(dir)
	}
	log.Info("Begin create node %s", nodePath)
	return self.Create(nodePath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (self *ZkConn) Touch(nodePath string) (string, error) {
	return self.Create(nodePath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (self *ZkConn) CreatePersistenceNode(nodePath string, value string) (string, error) {
	return self.Create(nodePath, value, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

// recursive wrapper around delete
func (self *ZkConn) RecursiveDelete(dirPath string) error {
	if stat, err := self.Exists(dirPath); err != nil || stat == nil {
		return errors.New(fmt.Sprintf("Node at %s doesn't exist!", dirPath))
	}
	children, _, err := self.Children(dirPath)
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to list children. Error: %s.", err.Error()))
	}
	for _, child := range children {
		if err := self.RecursiveDelete(dirPath + "/" + child); err != nil {
			return err
		}
	}
	return self.Delete(dirPath, -1)
}

func (self *ZkConn) MoniterRingNodes(ringPath string) ([]<-chan zookeeper.Event, error) {
	partitions, _, err := self.Children(ringPath)
	if err != nil {
		return nil, err
	}
	var partitionsEventChan []<-chan zookeeper.Event
	for _, partition := range partitions {
		// moniter partition
		_, _, paritionChan, err := self.ChildrenW(partition)
		if err != nil {
			return nil, err
		}
		partitionsEventChan = append(partitionsEventChan, paritionChan)
	}
	return partitionsEventChan, nil
}
