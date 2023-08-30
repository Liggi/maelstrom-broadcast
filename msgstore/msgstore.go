package msgstore

import (
	"log"
	"sync"
)

type SentMessage struct {
	MsgID      int
	DestNodeID string
	Messages   []int
}

type MessageStore struct {
	msgIDToMessage    map[int]SentMessage
	destNodeIDToMsgID map[string][]int
	mutex             sync.Mutex
}

func NewMessageStore() *MessageStore {
	return &MessageStore{
		msgIDToMessage:    map[int]SentMessage{},
		destNodeIDToMsgID: map[string][]int{},
	}
}

// TODO: I've changed `message` to `messages`, we always store it as a slice now. There might be knock-on effects. Come back to this.

func (s *MessageStore) AddMessage(nodeID string, newMessageID int, messages []int) {
	log.Printf("storing message (%v) sent to %s", messages, nodeID)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	sentMessage := SentMessage{
		MsgID:      newMessageID,
		DestNodeID: nodeID,
		Messages:   messages,
	}

	s.msgIDToMessage[newMessageID] = sentMessage
	s.destNodeIDToMsgID[nodeID] = append(s.destNodeIDToMsgID[nodeID], newMessageID)
}

func (s *MessageStore) GetMessageById(id int) (SentMessage, bool) {
	log.Printf("checking for stored message with id %d", id)
	message, ok := s.msgIDToMessage[id]
	return message, ok
}

func (s *MessageStore) DeleteMessageById(id int) {
	log.Printf("deleting the stored message with id %d", id)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	message := s.msgIDToMessage[id]
	allMessageIdsForSameNode := s.destNodeIDToMsgID[message.DestNodeID]

	newMessageIdsForSameNode := []int{}
	for _, messageId := range allMessageIdsForSameNode {
		if messageId != id {
			newMessageIdsForSameNode = append(newMessageIdsForSameNode, messageId)
		}
	}
	s.destNodeIDToMsgID[message.DestNodeID] = newMessageIdsForSameNode

	delete(s.msgIDToMessage, id)
}

func (s *MessageStore) GetMessagesByDestNodeId(destNodeId string) []SentMessage {
	log.Printf("get stored messages for %s", destNodeId)
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
