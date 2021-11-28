// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package buffer

//FrameID is the type for frame id
type FrameID uint32

//ClockReplacer represents the clock replacer algorithm
type ClockReplacer struct {
	cList     *circularList
	clockHand **node
}

// Victim removes the victim frame as defined by the replacement policy
func (c *ClockReplacer) Victim() *FrameID {
	if c.cList.size == 0 {
		return nil
	}

	var victimFrameID *FrameID
	currentNode := (*c.clockHand)
	for {
		if currentNode.value.(bool) {
			currentNode.value = false
			c.clockHand = &currentNode.next
		} else {
			frameID := currentNode.key.(FrameID)
			victimFrameID = &frameID

			c.clockHand = &currentNode.next

			c.cList.remove(currentNode.key)
			return victimFrameID
		}
	}
}

//Unpin unpins a frame, indicating that it can now be victimized
func (c *ClockReplacer) Unpin(id FrameID) {
	if !c.cList.hasKey(id) {
		c.cList.insert(id, true)
		if c.cList.size == 1 {
			c.clockHand = &c.cList.head
		}
	}
}

//Pin pins a frame, indicating that it should not be victimized until it is unpinned
func (c *ClockReplacer) Pin(id FrameID) {
	node := c.cList.find(id)
	if node == nil {
		return
	}

	if (*c.clockHand) == node {
		c.clockHand = &(*c.clockHand).next
	}
	c.cList.remove(id)

}

//Size returns the size of the clock
func (c *ClockReplacer) Size() uint32 {
	return c.cList.size
}

//NewClockReplacer instantiates a new clock replacer
func NewClockReplacer(poolSize uint32) *ClockReplacer {
	cList := newCircularList(poolSize)
	return &ClockReplacer{cList, &cList.head}
}
