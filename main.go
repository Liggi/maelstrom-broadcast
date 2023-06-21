package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastRequest struct {
	Message int `json:"message"`
}

type genericResponse struct {
	Type string `json:"type"`
}

type readResponse struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type Node interface {
	Handle(string, maelstrom.HandlerFunc)
	Reply(maelstrom.Message, interface{}) error
	Run() error
}

type MessageManager struct {
	messages []int
	n        Node
}

func (m *MessageManager) Init() {
	m.n = maelstrom.NewNode()

	m.n.Handle("broadcast", m.HandleBroadcast)
	m.n.Handle("read", m.HandleRead)
	m.n.Handle("topology", m.HandleTopology)
}

func (m *MessageManager) Run() error {
	return m.n.Run()
}

func (m *MessageManager) HandleBroadcast(msg maelstrom.Message) error {
	var body broadcastRequest
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	m.messages = append(m.messages, body.Message)

	return m.n.Reply(msg, genericResponse{
		Type: "broadcast_ok",
	})
}

func (m *MessageManager) HandleRead(msg maelstrom.Message) error {
	return m.n.Reply(msg, readResponse{
		Type:     "read_ok",
		Messages: m.messages,
	})
}

func (m *MessageManager) HandleTopology(msg maelstrom.Message) error {
	return m.n.Reply(msg, genericResponse{
		Type: "topology_ok",
	})
}

func main() {
	manager := MessageManager{}
	manager.Init()

	if err := manager.Run(); err != nil {
		log.Fatal(err)
	}
}
