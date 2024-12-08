package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"

	pb "example.com/mapreduce/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	pb.UnimplementedWorkerServiceServer

	isMapper      bool
	isReducer     bool
	reducers      []*pb.ReducerInfo
	totalMappers  int32
	intervalStart int64
	intervalEnd   int64

	// Mapper state
	mapperOnce sync.Once

	// Reducer state
	mu            sync.Mutex
	receivedData  []int64
	mappersToWait int32 // how many mappers need to finish
	finished      bool
	BindAddress   string // to name output file
}

func (ws *Server) AssignRole(ctx context.Context, req *pb.AssignRoleRequest) (*pb.AssignRoleResponse, error) {
	ws.isMapper = req.IsMapper
	ws.isReducer = req.IsReducer
	ws.totalMappers = req.TotalMappers
	ws.intervalStart = req.IntervalStart
	ws.intervalEnd = req.IntervalEnd

	if ws.isMapper {
		ws.reducers = req.Reducers
	}
	if ws.isReducer {
		ws.mappersToWait = ws.totalMappers
	}

	role := "UNASSIGNED"
	if ws.isMapper {
		role = "MAPPER"
	} else if ws.isReducer {
		role = "REDUCER"
	}
	return &pb.AssignRoleResponse{Message: "Role: " + role}, nil
}

func (ws *Server) SendChunk(ctx context.Context, req *pb.SendChunkRequest) (*pb.SendChunkResponse, error) {
	if !ws.isMapper {
		return &pb.SendChunkResponse{Message: "Not a mapper"}, nil
	}

	// Mapper: we got a chunk of data
	values := req.Values
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	// Distribute values to reducers based on intervals
	for _, v := range values {
		target := ws.findReducer(v)
		if target == "" {
			log.Printf("Mapper: no reducer found for value %d, skipping", v)
			continue
		}
		err := ws.sendToReducer(target, []int64{v})
		if err != nil {
			log.Printf("Failed to send value %d to reducer %s: %v", v, target, err)
		}
	}

	// After finished sending, notify reducers we are done
	for _, r := range ws.reducers {
		err := ws.notifyMapperDone(r.Address)
		if err != nil {
			log.Printf("Failed to notify done to %s: %v", r.Address, err)
		}
	}

	return &pb.SendChunkResponse{Message: "Mapper finished sending data."}, nil
}

func (ws *Server) findReducer(val int64) string {
	for _, r := range ws.reducers {
		if val >= r.IntervalStart && val < r.IntervalEnd {
			return r.Address
		}
	}
	return ""
}

func (ws *Server) sendToReducer(addr string, values []int64) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}()
	client := pb.NewWorkerServiceClient(conn)
	_, err = client.SendMappedData(context.Background(), &pb.SendMappedDataRequest{
		Values:         values,
		ReducerAddress: addr,
	})
	return err
}

func (ws *Server) notifyMapperDone(addr string) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}()
	client := pb.NewWorkerServiceClient(conn)
	host, _ := os.Hostname()
	_, err = client.NotifyMapperDone(context.Background(), &pb.NotifyMapperDoneRequest{
		MapperAddress: host,
	})
	return err
}

func (ws *Server) SendMappedData(ctx context.Context, req *pb.SendMappedDataRequest) (*pb.Empty, error) {
	// Only reducers receive mapped data
	if !ws.isReducer {
		return &pb.Empty{}, nil
	}

	ws.mu.Lock()
	ws.receivedData = append(ws.receivedData, req.Values...)
	ws.mu.Unlock()
	return &pb.Empty{}, nil
}

func (ws *Server) NotifyMapperDone(ctx context.Context, req *pb.NotifyMapperDoneRequest) (*pb.Empty, error) {
	if !ws.isReducer {
		return &pb.Empty{}, nil
	}
	ws.mu.Lock()
	ws.mappersToWait--
	waiting := ws.mappersToWait
	ws.mu.Unlock()

	if waiting == 0 {
		// All mappers finished, finalize reduce
		ws.finalizeReduce()
	}

	return &pb.Empty{}, nil
}

func (ws *Server) finalizeReduce() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.finished {
		return
	}
	sort.Slice(ws.receivedData, func(i, j int) bool {
		return ws.receivedData[i] < ws.receivedData[j]
	})

	// Write to file
	outputFile := fmt.Sprintf("reducer_%s_output.txt", makeSafeFileName(ws.BindAddress))
	f, err := os.Create(outputFile)
	if err != nil {
		log.Printf("Reducer failed to create output file: %v", err)
		return
	}
	defer f.Close()
	for _, v := range ws.receivedData {
		fmt.Fprintln(f, v)
	}

	ws.finished = true
	log.Printf("Reducer %s wrote output to %s", ws.BindAddress, outputFile)
}

func makeSafeFileName(addr string) string {
	// Replace ':' with '_'
	return stringReplaceAll(addr, ":", "_")
}

func stringReplaceAll(s, old, new string) string {
	returnValue := ""
	for _, c := range s {
		if string(c) == old {
			returnValue += new
		} else {
			returnValue += string(c)
		}
	}
	return returnValue
}
