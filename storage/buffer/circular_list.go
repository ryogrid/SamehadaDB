// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package buffer

import (
	"errors"
	"fmt"
)

type node struct {
	key   interface{}
	value interface{}
	next  *node
	prev  *node
}

type circularList struct {
	head     *node
	tail     *node
	size     uint32
	capacity uint32
}

func (c *circularList) find(key interface{}) *node {
	ptr := c.head
	for i := uint32(0); i < c.size; i++ {
		if ptr.key == key {
			return ptr
		}

		ptr = ptr.next
	}

	return nil
}

func (c *circularList) hasKey(key interface{}) bool {
	return c.find(key) != nil
}

func (c *circularList) insert(key interface{}, value interface{}) error {
	if c.size == c.capacity {
		return errors.New("capacity is full")
	}

	newNode := &node{key, value, nil, nil}
	if c.size == 0 {
		newNode.next = newNode
		newNode.prev = newNode
		c.head = newNode
		c.tail = newNode
		c.size++
		return nil
	}

	node := c.find(key)
	if node != nil {
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

	return nil
}

func (c *circularList) remove(key interface{}) {
	node := c.find(key)
	if node == nil {
		return
	}

	if c.size == 1 {
		c.head = nil
		c.tail = nil
		c.size--
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
}

func (c *circularList) isFull() bool {
	return c.size == c.capacity
}

func (c *circularList) print() {
	if c.size == 0 {
		fmt.Println(nil)
	}
	ptr := c.head
	for i := uint32(0); i < c.size; i++ {
		fmt.Println(ptr.key, ptr.value, ptr.prev.key, ptr.next.key)
		ptr = ptr.next
	}
}

func newCircularList(maxSize uint32) *circularList {
	return &circularList{nil, nil, 0, maxSize}
}
