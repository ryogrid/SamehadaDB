// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package buffer

import (
	"fmt"
)

type node struct {
	key   FrameID //interface{}
	value bool    //interface{}
	next  *node
	prev  *node
}

type circularList struct {
	head       *node
	tail       *node
	size       uint32
	capacity   uint32
	supportMap map[FrameID]*node
}

func (c *circularList) hasKey(key FrameID) bool {
	_, ok := c.supportMap[key]
	return ok
}

func (c *circularList) insert(key FrameID, value bool) error {
	if c.size == c.capacity {
		panic("circularList::insert capacity is full")
	}

	newNode := &node{key, value, nil, nil}
	if c.size == 0 {
		newNode.next = newNode
		newNode.prev = newNode
		c.head = newNode
		c.tail = newNode
		c.size++
		c.supportMap[key] = newNode
		return nil
	}

	node, ok := c.supportMap[key]
	if ok {
		node.value = value
		return nil
	}

	newNode.next = c.head
	newNode.prev = c.tail

	c.tail.next = newNode
	if c.head == c.tail {
		c.head.next = newNode
	}

	c.tail = newNode
	c.head.prev = c.tail

	c.size++
	c.supportMap[key] = newNode

	return nil
}

func (c *circularList) remove(key FrameID) {
	node, ok := c.supportMap[key]
	if !ok {
		return
	}

	if c.size == 1 {
		c.head = nil
		c.tail = nil
		c.size--
		delete(c.supportMap, key)
		return
	}

	if node == c.head {
		c.head = c.head.next
	}

	if node == c.tail {
		c.tail = c.tail.prev
	}

	node.next.prev = node.prev
	node.prev.next = node.next

	c.size--
	delete(c.supportMap, key)
}

func (c *circularList) isFull() bool {
	return c.size == c.capacity
}

func (c *circularList) Print() {
	if c.size == 0 {
		fmt.Println("circularList is empty.")
	}
	ptr := c.head
	printStr := fmt.Sprintf("circularList size:%d supportMap len:%d |", c.size, len(c.supportMap))
	for i := uint32(0); i < c.size; i++ {
		printStr += fmt.Sprintf("-%v,%v,%v,%v-", ptr.key, ptr.value, ptr.prev.key, ptr.next.key)
		ptr = ptr.next
	}
	fmt.Println(printStr)
}

func newCircularList(maxSize uint32) *circularList {
	return &circularList{nil, nil, 0, maxSize, make(map[FrameID]*node)}
}
