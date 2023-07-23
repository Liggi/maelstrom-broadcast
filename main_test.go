package main

import (
	"log"
	"testing"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockNode struct {
	mock.Mock
}

func (m *MockNode) Handle(topic string, handler maelstrom.HandlerFunc) {
	m.Called(topic, handler)
}

func (m *MockNode) Reply(msg maelstrom.Message, body interface{}) error {
	args := m.Called(msg, body)
	return args.Error(0)
}

func (m *MockNode) Run() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockNode) Send(dest string, body interface{}) error {
	args := m.Called()
	return args.Error(0)
}

func TestMessageManager_HandleBroadcast(t *testing.T) {
	mockNode := &MockNode{}
	manager := &MessageManager{
		messages: []int{},
		n:        mockNode,
	}

	msg := maelstrom.Message{
		Body: []byte(`{"message": 1}`),
	}
	broadcastResp := genericResponse{Type: "broadcast_ok"}

	mockNode.On("Reply", msg, broadcastResp).Return(nil)

	err := manager.HandleBroadcast(msg)
	assert.Nil(t, err)
	assert.Equal(t, []int{1}, manager.messages)

	mockNode.AssertExpectations(t)
}

func TestRead(t *testing.T) {
	mockNode := &MockNode{}
	manager := &MessageManager{
		messages: []int{8, 12, 35},
		n:        mockNode,
	}

	msg := maelstrom.Message{
		Body: []byte(`{}`),
	}
	readResp := readResponse{
		Type:     "read_ok",
		Messages: []int{8, 12, 35},
	}

	mockNode.On("Reply", msg, readResp).Return(nil)

	err := manager.HandleRead(msg)
	assert.Nil(t, err)

	mockNode.AssertExpectations(t)
}

func TestTopology(t *testing.T) {
	mockNode := &MockNode{}
	manager := &MessageManager{
		n: mockNode,
	}

	msg := maelstrom.Message{
		Body: []byte(`{ "type": "topology", "topology": { "n1": ["n2"] } }`),
	}
	topologyResp := genericResponse{
		Type: "topology_ok",
	}

	mockNode.On("Reply", msg, topologyResp).Return(nil)

	err := manager.HandleTopology(msg)
	assert.Nil(t, err)

	mockNode.AssertExpectations(t)
}

func TestTopology_Change(t *testing.T) {
	mockNode := &MockNode{}

	// Need to initialise the NodeID
	manager := &MessageManager{
		n:      mockNode,
		nodeID: "n1",
	}
	topologyResp := genericResponse{
		Type: "topology_ok",
	}

	msg1 := maelstrom.Message{
		Body: []byte(`{ "type": "topology", "topology": { "n1": ["n2"] } }`),
	}

	mockNode.On("Reply", msg1, topologyResp).Return(nil)

	_ = manager.HandleTopology(msg1)

	msg2 := maelstrom.Message{
		Body: []byte(`{ "type": "topology", "topology": { "n1": ["n2", "n3"] } }`),
	}

	mockNode.On("Reply", msg2, topologyResp).Return(nil)

	_ = manager.HandleTopology(msg2)

	log.Fatal()
}
