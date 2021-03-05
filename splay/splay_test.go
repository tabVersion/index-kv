package splay

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestSplay(t *testing.T) {
	splayTree := new(Tree)
	for i := 0; i < 20; i++ {
		err := Insert(splayTree, rand.Uint32()%10000, nil)
		if err != nil {
			fmt.Println(err)
		} else {
			PrintTree(splayTree)
		}
		println("==========\n")
	}
}

func TestTree(t *testing.T) {
	splayTree := new(Tree)
	var idx []int
	for i := 0; i < 20; i++ {
		idx = append(idx, i)
		err := Insert(splayTree, uint32(i), nil)
		if err != nil {
			fmt.Println(err)
		}
	}

	Access(splayTree, 3)
	Access(splayTree, 10)
	var buf string
	Preorder(splayTree.root, &buf)
	if buf != "10-3-2-1-0-8-6-4-5-7-9-16-12-11-14-13-15-18-17-19-" {
		panic("unexpected preorder")
	}
}
