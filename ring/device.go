package ring

import (
	_ "fmt"
)

/*
 ======  ===============================================================
    id      unique integer identifier amongst devices
    weight  a float of the relative weight of this device as compared to
    others; this indicates how many partitions the builder will try
    to assign to this device
    zone    integer indicating which zone the device is in; a given
    partition will not be assigned to multiple devices within the
    same zone ip the ip address of the device
    port    the tcp port of the device
    device  the device's name on disk (sdb1, for example)
    meta    general use 'extra' field; for example: the online date, the
                hardware description
======  ===============================================================
*/
type device struct {
	id          int
	host        string
	weight      int
	zone        string
	parts       int
	partsWanted int
	description string
	sort_key    string
}

type deviceInfo struct {
	host        string
	weight      int
	zone        string
	description string
}

/*
type Interface interface {
        // Len is the number of elements in the collection.
        Len() int
        // Less returns whether the element with index i should sort
        // before the element with index j.
        Less(i, j int) bool
        // Swap swaps the elements with indexes i and j.
        Swap(i, j int)
}
*/
type devices []*device

// inverse order
func (self devices) Len() int           { return len(self) }
func (self devices) Less(i, j int) bool { return self[j].sort_key < self[i].sort_key }
func (self devices) Swap(i, j int)      { self[i], self[j] = self[j], self[i] }

// put the device to the right place
func (self devices) Order(dev *device, start int) {
	if dev.sort_key > self[start].sort_key {
		return
	}
	// binary search the place
	index := binarySearch(self, dev, start, len(self)-1)
	// [start-1 ~ index - 1] move forward
	for i := start; i <= index; i++ {
		self[i-1] = self[i]
	}
	// place the element
	self[index] = dev

}

func binarySearch(devs devices, dev *device, start int, end int) int {

	for start < end {
		m := (start + end) / 2
		if devs[m].sort_key == dev.sort_key {
			return m
		} else if devs[m].sort_key > dev.sort_key {
			start = m + 1
		} else {
			end = m - 1
		}
	}
	return start
}
