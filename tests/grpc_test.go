package tests

//
//func BenchmarkGRPC_Call(b *testing.B) {
//	addr := testGetAddr()
//	go func() {
//		listen, err := net.Listen("tcp", addr)
//		if err != nil {
//			fmt.Printf("failed to listen:%v", err)
//		}
//		//实现gRPC Server
//		s := grpc.NewServer()
//		//注册helloServer为客户端提供服务
//		pb.RegisterHelloServer(s, HelloServer) //内部调用了s.RegisterServer()
//		fmt.Println("Listen on" + Address)
//
//		s.Serve(listen)
//	}()
//
//}
