package ring

import (
	"fmt"
	"math/rand"
	// "strings"
	"sort"
	"testing"
)

func TestSort(t *testing.T) {
	/*var devs devices
	weightedParts := 2
	parts := []int{3, 2, 4, 5, 6}
	for i := 0; i < 5; i++ {
		dev := new(device)
		dev.id = i
		dev.partsWanted = parts[i] * weightedParts
		dev.sort_key = fmt.Sprintf("%08d.%04d", dev.partsWanted, rand.Int())
		devs = append(devs, dev)
	}
	sort.Sort(devs) //  id4:6 id3:5 id2:4 id1:3 id0:2
	if devs[0].id != 4 {
		t.Errorf("the biggest one's id is %d", devs[0].id)
	}

	// pop
	j := 1
	dev := devs[j]      // id3:5
	dev.partsWanted = 1 // id3:1
	dev.sort_key = fmt.Sprintf("%08d.%04d", dev.partsWanted, rand.Int())
	devs.Order(dev, j+1) // id4:6 id2:4 id0:3 id1:2 id3:1
	if devs[j].id != 2 {
		t.Errorf("the second one's id is %d", devs[j].id)
	}
	if devs[4].id != 3 {
		t.Errorf("the forth one's id is %d", devs[4].partsWanted)
	}*/
	var devs devices
	parts := []int{4, 8, 2}
	for i := 0; i < 3; i++ {
		dev := new(device)
		dev.id = i
		dev.partsWanted = parts[i]
		dev.sort_key = fmt.Sprintf("%08d.%04d", dev.partsWanted, rand.Int())
		devs = append(devs, dev)
	}
	sort.Sort(devs)
	devs[0].partsWanted = 7
	devs[0].sort_key = fmt.Sprintf("%08d.%04d", devs[0].partsWanted, rand.Int())
	devs.Order(devs[0], 1)
	if devs[0].partsWanted != 7 {
		t.Errorf("don't move error")
	}

	devs[0].partsWanted = 1
	devs[0].sort_key = fmt.Sprintf("%08d.%04d", devs[0].partsWanted, rand.Int())
	devs.Order(devs[0], 1)
	if devs[2].partsWanted != 1 {
		t.Errorf("move error want %d\n", devs[0].partsWanted)
	}

}
