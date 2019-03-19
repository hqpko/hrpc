package tests

import (
	"context"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
)

var (
	grpcAddr = "127.0.0.1:12006"
	grpcOnce = new(sync.Once)
)

type grpcServer struct{}

// SayHello implements helloworld.GreeterServer
func (s *grpcServer) SayHello(ctx context.Context, in *HelloRequest) (*HelloReply, error) {
	return &HelloReply{B: in.A + 1}, nil
}

func Benchmark_grpc_Call(b *testing.B) {
	startGRpcServer()

	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	c := NewGRpcReqClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	b.StartTimer()
	defer b.StopTimer()
	req := &HelloRequest{A: 1}
	for i := 0; i < b.N; i++ {
		_, err := c.SayHello(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func startGRpcServer() {
	grpcOnce.Do(func() {
		go func() {
			lis, err := net.Listen("tcp", grpcAddr)
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}
			s := grpc.NewServer()
			RegisterGRpcReqServer(s, &grpcServer{})
			if err := s.Serve(lis); err != nil {
				log.Fatalf("failed to serve: %v", err)
			}
		}()

		time.Sleep(100 * time.Millisecond)
	})
}
