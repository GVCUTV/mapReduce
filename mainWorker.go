package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"

	"mapreduce/worker"

	"google.golang.org/grpc"
	pb "mapreduce/proto"
)

func main() {
	var port string
	flag.StringVar(&port, "port", ":50051", "Worker listen port")
	flag.Parse()

	ws := &worker.WorkerServer{}
	ws.BindAddress = port

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServiceServer(grpcServer, ws)

	go func() {
		log.Printf("Worker listening on %s", port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Println("Received shutdown signal, shutting down...")
	grpcServer.GracefulStop()
}
