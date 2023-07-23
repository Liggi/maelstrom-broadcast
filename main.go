package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastRequest struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
	MsgID   int    `json:"msg_id"`
}

type broadcastResponse struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
}

type initRequest struct {
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

type topologyRequest struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
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
	Send(dest string, body any) error
	RPC(dest string, body any, handler maelstrom.HandlerFunc) error
	Run() error
}

type SentMessage struct {
	msgID      int
	destNodeID string
	message    int
}

type MessageStore struct {
	msgIDToMessage    map[int]SentMessage
	destNodeIDToMsgID map[string][]int
	mutex             sync.Mutex
}

type MessageManager struct {
	messages      []int
	topology      map[string][]string
	n             Node
	nodeID        string
	messageStore  *MessageStore
	lastMessageID int
}

type DebugLogger struct {
	active bool
}

var debugLogger *DebugLogger

func NewDebugLogger() *DebugLogger {
	return &DebugLogger{}
}

func (dl *DebugLogger) EnableDebug() {
	dl.active = true
}

func (dl *DebugLogger) Log(message string) {
	if dl.active {
		log.Print(message)
	}
}

func NewMessageStore() *MessageStore {
	return &MessageStore{
		msgIDToMessage:    map[int]SentMessage{},
		destNodeIDToMsgID: map[string][]int{},
	}
}

func (s *MessageStore) AddMessage(nodeID string, newMessageID, message int) {
	debugLogger.Log(fmt.Sprintf("Storing message (%d) sent to %s", message, nodeID))
	s.mutex.Lock()
	defer s.mutex.Unlock()

	sentMessage := SentMessage{
		msgID:      newMessageID,
		destNodeID: nodeID,
		message:    message,
	}

	s.msgIDToMessage[newMessageID] = sentMessage
	s.destNodeIDToMsgID[nodeID] = append(s.destNodeIDToMsgID[nodeID], newMessageID)
}

func (s *MessageStore) GetMessageById(id int) (SentMessage, bool) {
	debugLogger.Log(fmt.Sprintf("Do we have a stored message with id %d?", id))
	message, ok := s.msgIDToMessage[id]
	return message, ok
}

func (s *MessageStore) DeleteMessageById(id int) {
	debugLogger.Log(fmt.Sprintf("Delete the stored message with id %d", id))
	s.mutex.Lock()
	defer s.mutex.Unlock()

	message := s.msgIDToMessage[id]
	allMessageIdsForSameNode := s.destNodeIDToMsgID[message.destNodeID]

	newMessageIdsForSameNode := []int{}
	for _, messageId := range allMessageIdsForSameNode {
		if messageId != id {
			newMessageIdsForSameNode = append(newMessageIdsForSameNode, messageId)
		}
	}
	s.destNodeIDToMsgID[message.destNodeID] = newMessageIdsForSameNode

	delete(s.msgIDToMessage, id)
}

func (s *MessageStore) GetMessagesByDestNodeId(destNodeId string) []SentMessage {
	debugLogger.Log(fmt.Sprintf("Get stored messages for %s", destNodeId))
	s.mutex.Lock()
	defer s.mutex.Unlock()

	messageIDs, ok := s.destNodeIDToMsgID[destNodeId]

	if !ok {
		return nil
	}

	messages := make([]SentMessage, len(messageIDs))
	for i, id := range messageIDs {
		messages[i] = s.msgIDToMessage[id]
	}

	return messages
}

func (m *MessageManager) Init() {
	debugLogger.Log("Init()")
	m.n = maelstrom.NewNode()
	m.messageStore = NewMessageStore()

	m.n.Handle("init", m.HandleInit)
	m.n.Handle("broadcast", m.HandleBroadcast)
	m.n.Handle("read", m.HandleRead)
	m.n.Handle("topology", m.HandleTopology)
}

func (m *MessageManager) Run() error {
	debugLogger.Log("Run()")
	return m.n.Run()
}

func (m *MessageManager) HandleInit(msg maelstrom.Message) error {
	debugLogger.Log("HandleInit()")
	var body initRequest
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	m.nodeID = body.NodeID

	return m.n.Reply(msg, genericResponse{
		Type: "init_ok",
	})
}

func (m *MessageManager) HandleBroadcastOk(msg maelstrom.Message) error {
	debugLogger.Log(fmt.Sprintf("%s replied to our message broadcast", msg.Src))

	var body broadcastResponse
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	debugLogger.Log(fmt.Sprintf("Checking for stored messages for %s", msg.Src))

	// Which node is responding? Do we have any stored messages for it?
	storedMessages := m.messageStore.GetMessagesByDestNodeId(msg.Src)

	// Loop through all the messages and send them (excluding this one, obviously)
	for _, message := range storedMessages {
		if message.msgID == body.InReplyTo {
			continue
		}

		debugLogger.Log(fmt.Sprintf("We have a message for %s", msg.Src))

		newMessageId := m.lastMessageID + 1
		m.lastMessageID = newMessageId

		newBroadcastRequest := broadcastRequest{
			Type:    "broadcast",
			Message: message.message,
			MsgID:   newMessageId,
		}

		debugLogger.Log(fmt.Sprintf("Sending %d to %s", message.message, msg.Src))

		m.n.RPC(msg.Src, newBroadcastRequest, func(msg maelstrom.Message) error { return nil })
	}

	// Remove the message from our store
	m.messageStore.DeleteMessageById(body.InReplyTo)

	return nil
}

func (m *MessageManager) HandleBroadcast(msg maelstrom.Message) error {
	var body broadcastRequest
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	debugLogger.Log(fmt.Sprintf("Received a (%d) from %s", body.Message, msg.Src))

	seenBefore := false
	for _, message := range m.messages {
		if message == body.Message {
			seenBefore = true
		}
	}

	if seenBefore {
		debugLogger.Log(fmt.Sprintf("Replying to %s with ok", msg.Src))
		return m.n.Reply(msg, broadcastResponse{
			Type:      "broadcast_ok",
			InReplyTo: body.MsgID,
		})
	}

	m.messages = append(m.messages, body.Message)

	// Broadcast this to every node in the topology (except the sender)
	for _, nodeID := range m.topology[m.nodeID] {
		if nodeID == msg.Src {
			continue
		}

		newMessageId := m.lastMessageID + 1
		m.lastMessageID = newMessageId

		m.messageStore.AddMessage(nodeID, newMessageId, body.Message)

		debugLogger.Log("Finishing adding message")

		newBroadcastRequest := broadcastRequest{
			Type:    "broadcast",
			Message: body.Message,
			MsgID:   newMessageId,
		}

		debugLogger.Log(fmt.Sprintf("Gossipping %d to %s", body.Message, nodeID))

		m.n.RPC(nodeID, newBroadcastRequest, m.HandleBroadcastOk)
	}

	response := broadcastResponse{
		Type:      "broadcast_ok",
		InReplyTo: body.MsgID,
	}

	debugLogger.Log(fmt.Sprintf("Replying to %s with ok", msg.Src))

	return m.n.Reply(msg, response)
}

func (m *MessageManager) HandleRead(msg maelstrom.Message) error {
	debugLogger.Log("Responding to Read")
	return m.n.Reply(msg, readResponse{
		Type:     "read_ok",
		Messages: m.messages,
	})
}

func (m *MessageManager) HandleTopology(msg maelstrom.Message) error {
	debugLogger.Log("Responding to Topology")
	var body topologyRequest
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}

	m.topology = body.Topology

	return m.n.Reply(msg, genericResponse{
		Type: "topology_ok",
	})
}

func main() {
	debugLogger = NewDebugLogger()
	debugLogger.EnableDebug()

	manager := MessageManager{}
	manager.Init()

	if err := manager.Run(); err != nil {
		log.Fatal(err)
	}
}
