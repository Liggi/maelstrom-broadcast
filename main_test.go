package main

import (
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
	// Test that a `read_ok` response happens

	// Test that the messages collection is returned
}

func TestTopology(t *testing.T) {
	// Test that a `topology_ok` response happens
}
