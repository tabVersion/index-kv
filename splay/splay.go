package splay

import (
	"fmt"
	"github.com/tabVersion/index-kv/chunk"
	"log"
	"strconv"
	"strings"
)

type Node struct {
	key    int
	value  *chunk.Chunk
	left   *Node
	right  *Node
	parent *Node
}

type Splay interface {
	SetRoot(n *Node)
	GetRoot() *Node
	Ord(key1, key2 int) int
}

type Tree struct {
	root *Node
}

func (t *Tree) GetRoot() *Node {
	return t.root
}

func (t *Tree) SetRoot(root *Node) {
	t.root = root
}

func (t *Tree) Ord(key1 int, key2 int) int {
	if key1 < key2 {
		return 0
	} else if key1 == key2 {
		return 1
	} else {
		return 2
	}
}

func FindNode(s Splay, key int, root *Node) *Node {
	if root == nil {
		return nil
	} else {
		switch s.Ord(key, root.key) {
		case 0:
			return FindNode(s, key, root.left)
		case 1:
			return root
		case 2:
			return FindNode(s, key, root.right)
		}
		return nil
	}
}

func Insert(s Splay, key int, value *chunk.Chunk) error {
	if FindNode(s, key, s.GetRoot()) != nil {
		log.Fatalf("[splay.splay.Insert] key %v alright exist", key)
	}
	n := insertNode(s, key, value, s.GetRoot())
	splay(s, n)
	log.Printf("[splay.splay.Insert] insert key: %d", key)
	return nil
}

func insertNode(s Splay, key int, value *chunk.Chunk, root *Node) *Node {
	if root == nil {
		n := new(Node)
		n.key = key
		n.value = value
		s.SetRoot(n)
		return s.GetRoot()
	}

	switch s.Ord(key, root.key) {
	case 0:
		if root.left == nil {
			root.left = new(Node)
			root.left.key = key
			root.left.value = value
			root.left.parent = root
			return root.left
		} else {
			return insertNode(s, key, value, root.left)
		}
	case 2:
		if root.right == nil {
			root.right = new(Node)
			root.right.key = key
			root.right.value = value
			root.right.parent = root
			return root.right
		} else {
			return insertNode(s, key, value, root.right)
		}
	}
	return nil
}

func splay(s Splay, n *Node) {
	for n != s.GetRoot() {
		if n.parent == s.GetRoot() && n.parent.left == n {
			zigL(s, n)
		} else if n.parent == s.GetRoot() && n.parent.right == n {
			zigR(s, n)
		} else if n.parent.left == n && n.parent.parent.left == n.parent {
			zigZigL(s, n)
		} else if n.parent.right == n && n.parent.parent.right == n.parent {
			zigZigR(s, n)
		} else if n.parent.right == n && n.parent.parent.left == n.parent {
			zigZagLR(s, n)
		} else {
			zigZagRL(s, n)
		}
	}
}

func Access(s Splay, key int) *Node {
	log.Printf("[splay.splay.Access] access key: %d", key)
	n := FindNode(s, key, s.GetRoot())
	splay(s, n)
	return n
}

func zigL(s Splay, n *Node) {
	n.parent.left = n.right
	if n.right != nil {
		n.right.parent = n.parent
	}
	n.parent.parent = n
	n.right = n.parent
	n.parent = nil

	s.SetRoot(n)
}

func zigR(s Splay, n *Node) {
	n.parent.right = n.left
	if n.left != nil {
		n.left.parent = n.parent
	}
	n.parent.parent = n
	n.left = n.parent
	n.parent = nil

	s.SetRoot(n)
}

func zigZigL(s Splay, n *Node) {
	gg := n.parent.parent.parent

	var isRoot, isLeft bool
	if gg == nil {
		isRoot = true
	} else {
		isRoot = false
		isLeft = gg.left == n.parent.parent
	}

	n.parent.parent.left = n.parent.right
	if n.parent.right != nil {
		n.parent.right.parent = n.parent.parent
	}
	n.parent.left = n.right
	if n.right != nil {
		n.right.parent = n.parent
	}
	n.parent.right = n.parent.parent
	n.parent.parent.parent = n.parent
	n.right = n.parent
	n.parent.parent = n
	n.parent = gg

	if isRoot == true {
		s.SetRoot(n)
	} else if isLeft == true {
		gg.left = n
	} else {
		gg.right = n
	}
}

func zigZigR(s Splay, n *Node) {
	gg := n.parent.parent.parent

	var isRoot, isLeft bool
	if gg == nil {
		isRoot = true
	} else {
		isRoot = false
		isLeft = gg.left == n.parent.parent
	}

	n.parent.parent.right = n.parent.left
	if n.parent.left != nil {
		n.parent.left.parent = n.parent.parent
	}
	n.parent.right = n.left
	if n.left != nil {
		n.left.parent = n.parent
	}
	n.parent.left = n.parent.parent
	n.parent.parent.parent = n.parent
	n.left = n.parent
	n.parent.parent = n
	n.parent = gg

	if isRoot == true {
		s.SetRoot(n)
	} else if isLeft == true {
		gg.left = n
	} else {
		gg.right = n
	}
}

func zigZagLR(s Splay, n *Node) {
	gg := n.parent.parent.parent

	var isRoot, isLeft bool
	if gg == nil {
		isRoot = true
	} else {
		isRoot = false
		isLeft = gg.left == n.parent.parent
	}

	n.parent.parent.left = n.right
	if n.right != nil {
		n.right.parent = n.parent.parent
	}
	n.parent.right = n.left
	if n.left != nil {
		n.left.parent = n.parent
	}
	n.left = n.parent
	n.right = n.parent.parent
	n.parent.parent.parent = n
	n.parent.parent = n
	n.parent = gg

	if isRoot == true {
		s.SetRoot(n)
	} else if isLeft == true {
		gg.left = n
	} else {
		gg.right = n
	}
}

func zigZagRL(s Splay, n *Node) {
	gg := n.parent.parent.parent

	var isRoot, isLeft bool
	if gg == nil {
		isRoot = true
	} else {
		isRoot = false
		isLeft = gg.left == n.parent.parent
	}

	n.parent.parent.right = n.left
	if n.left != nil {
		n.left.parent = n.parent.parent
	}
	n.parent.left = n.right
	if n.right != nil {
		n.right.parent = n.parent
	}
	n.right = n.parent
	n.left = n.parent.parent
	n.parent.parent.parent = n
	n.parent.parent = n
	n.parent = gg

	if isRoot == true {
		s.SetRoot(n)
	} else if isLeft == true {
		gg.left = n
	} else {
		gg.right = n
	}
}

// ===== printNode tree =====

func PrintTree(s Splay) {
	printNode(s.GetRoot(), 0)
}

func printNode(n *Node, depth int) {
	if n == nil {
		return
	}
	fmt.Println(strings.Repeat("-", 2 * depth), n.key)
	printNode(n.left, depth + 1)
	printNode(n.right, depth + 1)
}

func Preorder(root *Node, buf *string) {
	if root == nil {
		return
	}
	*buf += strconv.Itoa(root.key) + "-"
	Preorder(root.left, buf)
	Preorder(root.right, buf)
}
