package test

import (
	"testing"

	"github.com/hqpko/hrpc"
)

func TestRPC(t *testing.T) {
	s := hrpc.NewServer()
	s.Register(1, func() {

	})
}
