package zk

import (
	"errors"
	"github.com/senarukana/dldb/log"
	"launchpad.net/gozk/zookeeper"
	"path"
	"sort"
	"strings"
)

/** From http://zookeeper.apache.org/doc/r3.1.2/recipes.html#sc_recipes_Locks
 * Clients wishing to obtain a lock do the following:
 * 1. Call create() with a pathname of "_locknode_/lock-" and the sequence and ephemeral flags set.
 * 2. Call getChildren() on the lock node without setting the watch flag (this is important to avoid the herd
 *    effect).
 * 3. If the pathname created in step 1 has the lowest sequence number suffix, the client has the lock and the
 *    client exits the protocol.
 * 4. The client calls exists() with the watch flag set on the path in the lock directory with the next lowest
 *    sequence number.
 * 5. if exists() returns false, go to step 2. Otherwise, wait for a notification for the pathname from the
 *    previous step before going to step 2.
 *
 * The unlock protocol is very simple:
 *  clients wishing to release a lock simply delete the node they created in step 1
 *
 * Note that this is not goroutine-safe and will not work for directories with other children!
 */

type Mutex struct {
	zk   *zookeeper.Conn
	lock string
	Path string
}

func NewMutex(zk *zookeeper.Conn, path string) *Mutex {
	return &Mutex{zk: zk, Path: path}
}

func (self *Mutex) Lock() (err error) {
	self.lock, err = rlock(self.zk, self.Path, "lock-", []string{"lock-"})
	return err
}

func (self *Mutex) TryLock() (bool, error) {
	lock, getted, err := rtrylock(self.zk, self.Path, "lock-", []string{"lock-"})
	if getted {
		self.lock = lock
	}
	return getted, err
}

func (self *Mutex) Unlock() error {
	if self.lock == "" {
		return nil // lockPath isn't set, we haven't locked anything
	}
	err := self.zk.Delete(self.lock, -1)
	if err != nil {
		return err
	}
	self.lock = ""
	return nil
}

func rlock(zk *zookeeper.Conn, basePath, prefix string, checkPrefix []string) (lock string, err error) {
	//step 1
	lock, err = zk.Create(path.Join(basePath, prefix), "", zookeeper.EPHEMERAL|zookeeper.SEQUENCE,
		zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return lock, err
	}
	for {
		// step 2
		children, _, err := zk.Children(basePath)
		// oops, error occured, return
		if err != nil {
			return lock, err
		}
		// step 3
		if children == nil || len(children) == 0 {
			return lock, errors.New("get children didn't return my lock")
		}
		// filter out non-lock children and extract sequence numbers
		filteredChildren := map[string]string{}
		filteredChildrenKeys := []string{}
		for _, v := range children {
			for _, pfx := range checkPrefix {
				if strings.HasPrefix(v, pfx) {
					seqNum := strings.Replace(v, pfx, "", 1)
					filteredChildren[seqNum] = v
					filteredChildrenKeys = append(filteredChildrenKeys, seqNum)
				}
			}
		}
		sort.Strings(filteredChildrenKeys)
		prevLock := ""
		for _, seqNum := range filteredChildrenKeys {
			log.Trace("CHECKING SEQ %s: %s == %s", seqNum, path.Base(lock), filteredChildren[seqNum])
			if path.Base(lock) == filteredChildren[seqNum] {
				break
			}
			prevLock = path.Join(basePath, filteredChildren[seqNum])
		}
		// there is no previous lock, So I take the lock
		if prevLock == "" {
			return lock, nil
		}

		// step4
		stat, watchCh, err := zk.ExistsW(prevLock)
		// oops, error occured, return
		if err != nil {
			return lock, err
		}
		// step 5
		if stat == nil {
			continue
		}
		<-watchCh
	}
	return lock, nil
}

// not acquire the lock recursively
func rtrylock(zk *zookeeper.Conn, basePath, prefix string, checkPrefix []string) (string, bool, error) {
	// step 1
	lock, err := zk.Create(path.Join(basePath, prefix), "", zookeeper.EPHEMERAL|zookeeper.SEQUENCE,
		zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return lock, false, err
	}
	// step 2
	children, _, err := zk.Children(basePath)
	if err != nil {
		return lock, false, err
	}
	// step 3
	if children == nil || len(children) == 0 {
		return lock, false, errors.New("get children didn't return my lock")
	}
	// filter out non-lock children and extract sequence numbers
	filteredChildren := map[string]string{}
	filteredChildrenKeys := []string{}
	for _, v := range children {
		for _, pfx := range checkPrefix {
			if strings.HasPrefix(v, pfx) {
				seqNum := strings.Replace(v, pfx, "", 1)
				filteredChildren[seqNum] = v
				filteredChildrenKeys = append(filteredChildrenKeys, seqNum)
			}
		}
	}
	sort.Strings(filteredChildrenKeys)
	prevLock := ""
	for _, seqNum := range filteredChildrenKeys {
		log.Trace("CHECKING SEQ %s: %s == %s", seqNum, path.Base(lock), filteredChildren[seqNum])
		if path.Base(lock) == filteredChildren[seqNum] {
			break
		}
		prevLock = path.Join(basePath, filteredChildren[seqNum])
	}
	// there is no previous lock, So I take the lock
	if prevLock == "" {
		return lock, true, nil
	} else {
		return lock, false, nil
	}
}
