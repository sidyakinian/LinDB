package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type RequestBody struct {
	Type  string `json:"type"`
	Key   int    `json:"key"`
	Value int    `json:"value,omitempty"`
	From  int    `json:"from,omitempty"`
	To    int    `json:"to,omitempty"`
}

type server struct {
	node         *maelstrom.Node
	stateMachine *Map
	lock         sync.Mutex
}

type Map struct {
	data map[int]int
}

func NewMap() *Map {
	return &Map{data: make(map[int]int)}
}

func (m *Map) Apply(op RequestBody) (*Map, map[string]interface{}) {
	switch op.Type {
	case "read":
		val, exists := m.data[op.Key]
		if exists {
			return m, map[string]interface{}{
				"type":  "read_ok",
				"value": val,
			}
		}
		return m, map[string]interface{}{
			"type":    "error",
			"message": "not found",
		}
	case "write":
		newMap := &Map{data: make(map[int]int)}
		for k, v := range m.data {
			newMap.data[k] = v
		}
		newMap.data[op.Key] = op.Value
		return newMap, map[string]interface{}{
			"type": "write_ok",
		}
	case "cas":
		current, exists := m.data[op.Key]
		if !exists || current != op.From {
			return m, map[string]interface{}{
				"type":    "error",
				"message": "precondition failed",
			}
		}
		newMap := &Map{data: make(map[int]int)}
		for k, v := range m.data {
			newMap.data[k] = v
		}
		newMap.data[op.Key] = op.To
		return newMap, map[string]interface{}{
			"type": "cas_ok",
		}
	}
	return m, map[string]interface{}{
		"type":    "error",
		"message": "invalid operation",
	}
}

func main() {
	node := maelstrom.NewNode()
	s := &server{
		node:         node,
		stateMachine: NewMap(),
	}
	node.Handle("read", s.clientReq)
	node.Handle("write", s.clientReq)
	node.Handle("cas", s.clientReq)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) clientReq(message maelstrom.Message) error {
	var body RequestBody
	if err := json.Unmarshal(message.Body, &body); err != nil {
		return err
	}

	s.lock.Lock()
	newStateMachine, resp := s.stateMachine.Apply(body)
	s.stateMachine = newStateMachine
	s.lock.Unlock()

	return s.node.Reply(message, resp)
}