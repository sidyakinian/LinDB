package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
    maelstrom.Node
}

type RequestBody struct {
	Type  string `json:"type"`
	Key   int    `json:"key"`
	Value int    `json:"value,omitempty"`
	From  int    `json:"from,omitempty"`
	To    int    `json:"to,omitempty"`
	Term int `json:"term"`
	VoteGranted bool `json:"vote_granted"`
	LastLogTerm int `json:"last_log_term"`
	LastLogIndex int `json:"last_log_index"`
	CandidateID string `json:"candidate_id"`
}

type server struct {
	node         *Node
	stateMachine *Map
	lock         sync.Mutex
	state 	     string 	// follower, candidate, leader
	electionTimeout time.Duration
	electionDeadline time.Time
	term         int
	log 		 *ReplicatedLog
	votedFor    string
}

func newServer(node *Node) *server {
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
	s.votedFor = s.node.ID()
	s.resetElectionDeadline()
	log.Printf("became candidate for term %d", s.term)
	s.requestVotes()
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
		// No recursive locks in Go, avoid deadlock
		if time.Now().After(s.electionDeadline) {
			if s.state != "leader" {
				s.becomeCandidate()
			} else {
				s.resetElectionDeadline()
			}
		}
	}
}

func (s *server) advanceTerm(term int) {
	if term < s.term {
		panic("term cannot go backwards")
	}
	s.term = term
	s.votedFor = ""
}

func (s *server) maybeStepDown(remote_term int) {
	if remote_term > s.term {
		fmt.Printf("term %d is behind %d, stepping down", s.term, remote_term)
		s.advanceTerm(remote_term)
		s.becomeFollower()
	}
}

func (s *server) requestVotes() {
	votes := map[string]struct{}{
		s.node.ID(): {},
	}
	currentTerm := s.term

	handlerFunc := func(msg maelstrom.Message) error {
		var body RequestBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		s.maybeStepDown(body.Term)
		if s.state == "candidate" && s.term == currentTerm && s.term == body.Term && body.VoteGranted {
			// We have a vote for our candidacy, and we're still in the term we requested! Record the vote
			votes[msg.Src] = struct{}{}
			log.Printf("Have votes: %v", votes)
		}
		return nil
	}

	s.node.brpc(map[string]interface{}{
		"type":           "request_vote",
		"term":           currentTerm,
		"candidate_id":   s.node.ID(),
		"last_log_index": len(s.log.entries),
		"last_log_term":  s.log.entries[len(s.log.entries)-1].Term,
	}, handlerFunc)
}

func (s *server) becomeLeader() {
	if s.state != "candidate" {
		panic("cannot become leader if not a candidate")
	}

	s.state = "leader"
	log.Printf("became leader for term %d", s.term)
}

func (n *Node) otherNodeIDs() []string {
	otherIDs := []string{}
	for _, id := range n.NodeIDs() {
		if id != n.ID() {
			otherIDs = append(otherIDs, id)
		}
	}
	return otherIDs
}

func (n *Node) brpc(body map[string]any, handler maelstrom.HandlerFunc) {
	for _, node := range n.otherNodeIDs() {
		go n.RPC(node, body, handler)
	}
}

type Map struct {
	data map[int]int
}

func NewMap() *Map {
	return &Map{data: make(map[int]int)}
}

func (s *server) Apply(m *Map, op RequestBody, message maelstrom.Message) (*Map, map[string]interface{}) {
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
	case "request_vote":
		s.maybeStepDown(op.Term)
		grant := false

		if op.Term < s.term {
			log.Printf("Candidate term %d lower than %d, not granting vote.", op.Term, s.term)
		} else if s.votedFor != "" {
			log.Printf("Already voted for %s; not granting vote.", s.votedFor)
		} else if op.LastLogTerm < s.log.entries[len(s.log.entries)-1].Term { 
			log.Printf("Have log entries from term %d, which is newer than remote term %d; not granting vote.", s.log.entries[len(s.log.entries)-1].Term, op.LastLogTerm)
		} else if op.LastLogTerm == s.log.entries[len(s.log.entries)-1].Term && op.LastLogIndex < len(s.log.entries) {
			log.Printf("Our logs are both at term %d, but our log is %d and theirs is only %d long; not granting vote.", s.log.entries[len(s.log.entries)-1].Term, len(s.log.entries), op.LastLogIndex)
		} else {
			log.Printf("Granting vote to %s", op.CandidateID)
			grant = true
			s.votedFor = op.CandidateID
			s.resetElectionDeadline() 
		}
	
		// Sending the reply
		s.node.Reply(message, map[string]interface{}{
			"type":         "request_vote_res",
			"term":         s.term,
			"vote_granted": grant,
		})
	}
	return m, map[string]interface{}{
		"type":    "error",
		"message": "invalid operation",
	}
}

func main() {
	maelstromNode := maelstrom.NewNode()
	node := Node{
		Node: *maelstromNode,
	}
	s := newServer(&node)
	
	node.Handle("read", s.clientReq)
	node.Handle("write", s.clientReq)
	node.Handle("cas", s.clientReq)
	node.Handle("request_vote", s.clientReq)

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
	newStateMachine, resp := s.Apply(s.stateMachine, body, message)
	s.stateMachine = newStateMachine
	s.lock.Unlock()

	return s.node.Reply(message, resp)
}