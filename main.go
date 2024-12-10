package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"mapreduce/master"
	"mapreduce/worker"

	"google.golang.org/grpc"
	pb "mapreduce/proto"
)

func main() {
	var mode string
	var port string
	var configPath string
	var inputPath string
	flag.StringVar(&mode, "mode", "master", "Mode to run: master or worker")
	flag.StringVar(&port, "port", ":50051", "Worker listen port (only used in worker mode)")
	flag.StringVar(&configPath, "config", "config.yaml", "Path to configuration file (only used in master mode)")
	flag.StringVar(&inputPath, "input", "input.txt", "Path to input file (only used in master mode)")
	flag.Parse()

	switch mode {
	case "master":
		if configPath == "" || inputPath == "" {
			fmt.Println("Usage: go run main.go --mode=master --config=config.yaml --input=input.txt")
			return
		}
		master.RunMaster(configPath, inputPath)
	case "worker":
		if port == "" {
			fmt.Println("Usage: go run main.go --mode=worker --port=:50051")
			return
		}
		runWorker(port)
	default:
		log.Fatalf("Unknown mode: %s "+
			"\nUsage"+
			"\nmaster: go run main.go --mode=master --config=config.yaml --input=input"+
			"\nworker: go run main.go --mode=worker --port=:50051", mode)

	}
}

func runWorker(port string) {
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
