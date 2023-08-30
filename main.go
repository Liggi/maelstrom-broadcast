package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"maelstrom-broadcast/msgstore"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastRequest struct {
	Type     string `json:"type"`
	Message  int    `json:"message,omitempty"`
	Messages []int  `json:"messages,omitempty"`
	MsgID    int    `json:"msg_id"`
}

type Node interface {
	Handle(string, maelstrom.HandlerFunc)
	Reply(maelstrom.Message, interface{}) error
	Send(dest string, body any) error
	RPC(dest string, body any, handler maelstrom.HandlerFunc) error
	Run() error
}

type MessageManager struct {
	messages      []int
	topology      map[string][]string
	n             Node
	nodeID        string
	messageStore  *msgstore.MessageStore
	lastMessageID int
}

func (m *MessageManager) Init() {
	m.n = maelstrom.NewNode()
	m.messageStore = msgstore.NewMessageStore()

	m.n.Handle("init", m.HandleInit)
	m.n.Handle("broadcast", m.HandleBroadcast)
	m.n.Handle("read", m.HandleRead)
	m.n.Handle("topology", m.HandleTopology)
}

func (m *MessageManager) Run() error {
	return m.n.Run()
}

func (m *MessageManager) HandleInit(msg maelstrom.Message) error {
	var body struct {
		NodeID  string   `json:"node_id"`
		NodeIDs []string `json:"node_ids"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	m.nodeID = body.NodeID

	return nil
}

func (m *MessageManager) HandleBroadcastOk(msg maelstrom.Message) error {
	log.Printf("%s replied to our message broadcast", msg.Src)

	var body struct {
		Type      string `json:"type"`
		InReplyTo int    `json:"in_reply_to"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	log.Printf("checking local message store for messages for %s", msg.Src)

	// Which node is responding? Do we have any stored messages for it?
	storedMessages := m.messageStore.GetMessagesByDestNodeId(msg.Src)

	// IDEA: Batch all of these messages and send them together, rather than firing a ton of individual RPCs

	// Loop through all the messages and send them (excluding this one, obviously)
	var allMessages []int
	for _, message := range storedMessages {
		if message.MsgID == body.InReplyTo {
			continue
		}

		log.Printf("local message store contains message for %s", msg.Src)

		allMessages = append(allMessages, message.Messages...)
	}

	newMessageId := m.lastMessageID + 1
	m.lastMessageID = newMessageId

	newBroadcastRequest := BroadcastRequest{
		Type:     "broadcast",
		Messages: allMessages,
		MsgID:    newMessageId,
	}

	log.Printf("sending %v to %s", allMessages, msg.Src)

	m.n.RPC(msg.Src, newBroadcastRequest, func(msg maelstrom.Message) error { return nil })

	// Remove the message from our store
	m.messageStore.DeleteMessageById(body.InReplyTo)

	return nil
}

func (m *MessageManager) HandleBroadcast(msg maelstrom.Message) error {
	var body BroadcastRequest
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	var messagesToProcess []int
	if body.Messages != nil {
		messagesToProcess = body.Messages
	} else {
		messagesToProcess = []int{body.Message}
	}

	log.Printf("received a (%v) from %s", messagesToProcess, msg.Src)

	unseen := findUnseenMessages(messagesToProcess, m.messages)

	if len(unseen) > 0 {
		m.messages = append(m.messages, unseen...)
	}

	log.Printf("replying to %s with ok", msg.Src)

	err = m.n.Reply(msg, struct {
		Type      string `json:"type"`
		InReplyTo int    `json:"in_reply_to"`
	}{
		Type:      "broadcast_ok",
		InReplyTo: body.MsgID,
	})
	if err != nil {
		return err
	}

	if len(unseen) == 0 {
		return nil
	}

	for _, nodeID := range m.topology[m.nodeID] {
		if nodeID == msg.Src {
			continue
		}

		newMessageId := m.lastMessageID + 1
		m.lastMessageID = newMessageId

		m.messageStore.AddMessage(nodeID, newMessageId, messagesToProcess)

		log.Println("successfully added messages to local message store")

		newBroadcastRequest := BroadcastRequest{
			Type:     "broadcast",
			Messages: messagesToProcess,
			MsgID:    newMessageId,
		}

		log.Printf("gossipping %v to %s", messagesToProcess, nodeID)

		m.n.RPC(nodeID, newBroadcastRequest, m.HandleBroadcastOk)
	}

	return nil
}

func (m *MessageManager) HandleRead(msg maelstrom.Message) error {
	log.Println("responding to read")

	return m.n.Reply(msg, struct {
		Type     string `json:"type"`
		Messages []int  `json:"messages"`
	}{
		Type:     "read_ok",
		Messages: m.messages,
	})
}

func (m *MessageManager) HandleTopology(msg maelstrom.Message) error {
	log.Println("responding to topology")
	var body struct {
		Type     string              `json:"type"`
		Topology map[string][]string `json:"topology"`
	}
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	m.topology = body.Topology

	return m.n.Reply(msg, struct {
		Type string `json:"type"`
	}{
		Type: "topology_ok",
	})
}

func main() {
	debug := flag.Bool("debug", false, "enable debug logs")
	flag.Parse()

	if !*debug {
		log.SetOutput(io.Discard)
	}

	manager := MessageManager{}
	manager.Init()

	if err := manager.Run(); err != nil {
		log.Fatal(err)
	}
}

func findUnseenMessages(messagesToProcess []int, existingMessages []int) []int {
	unseen := []int{}

	existingMessagesMap := make(map[int]bool)
	for _, message := range existingMessages {
		existingMessagesMap[message] = true
	}

	for _, message := range messagesToProcess {
		if _, exists := existingMessagesMap[message]; !exists {
			unseen = append(unseen, message)
		}
	}

	return unseen
}
