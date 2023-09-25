package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

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
	state 	     string 	// follower, candidate, leader
	electionTimeout time.Duration
	electionDeadline time.Time
	term         int
}

func newServer(node *maelstrom.Node) *server {
	log.Printf("new server is being initialized")
	s := &server{
		node: node,
		stateMachine: NewMap(),
		state: "follower",
		electionTimeout: 2 * time.Second,
		electionDeadline: time.Now(),
		term: 0,
	}
	go s.runElectionTask()
	return s
}

func (s *server) becomeCandidate() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.state = "candidate"
	s.advanceTerm(s.term + 1)
	s.resetElectionDeadline()

	log.Printf("became candidate for term %d", s.term)
}

func (s *server) becomeFollower() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.state = "follower"
	s.resetElectionDeadline()
	log.Printf("became follower for term %d", s.term)
}

func (s *server) resetElectionDeadline() {
	randomDelay := time.Duration((1 + float64(rand.Intn(100)/100.0)) * float64(s.electionTimeout))
	s.electionDeadline = time.Now().Add(randomDelay)
}

func (s *server) runElectionTask() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// s.lock.Lock() // No recursive locks in Go, avoid deadlock
		if time.Now().After(s.electionDeadline) {
			if s.state != "leader" {
				s.becomeCandidate()
			} else {
				s.resetElectionDeadline()
			}
		}
		// s.lock.Unlock()
	}
}

func (s *server) advanceTerm(term int) {
	if term < s.term {
		panic("term cannot go backwards")
	}
	s.term = term
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
	s := newServer(node)
	
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