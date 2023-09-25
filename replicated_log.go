package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Entry struct {
	Term int
	Op   Operation
}

type Operation struct {
	
}

type ReplicatedLog struct {
	entries []*Entry
	node    *maelstrom.Node
}

func NewLog(node *maelstrom.Node) *ReplicatedLog {
	// Create a log with a default entry
	return &ReplicatedLog{
		entries: []*Entry{
			{
				Term: 0,
				Op:   Operation{},
			},
		},
		node: node,
	}
}

func (l *ReplicatedLog) Get(index int) *Entry {
	if index <= 0 || index > len(l.entries) {
		return nil
	}
	return l.entries[index-1]
}

func (l *ReplicatedLog) Append(entries ...*Entry) {
	for _, e := range entries {
		l.entries = append(l.entries, e)
	}
	log.Printf("Log: %v", l.entries)
}

func (l *ReplicatedLog) Last() *Entry {
	return l.entries[len(l.entries)-1]
}

func (l *ReplicatedLog) Size() int {
	return len(l.entries)
}