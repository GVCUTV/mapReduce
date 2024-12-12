package master

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
	"log"
	pb "mapreduce/proto"
	"os"
)

type Config struct {
	Workers  []string `yaml:"workers"`
	Mappers  int      `yaml:"mappers"`
	Reducers int      `yaml:"reducers"`
}

// load the configuration file
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// read the input file
func readInput(path string) ([]int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := splitLines(string(data))
	var nums []int64
	for _, line := range lines {
		if line == "" {
			continue
		}
		var n int64
		_, err := fmt.Sscan(line, &n)
		if err != nil {
			return nil, err
		}
		nums = append(nums, n)
	}
	return nums, nil
}

// split the lines of input file
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i, c := range s {
		if c == '\n' || c == '\r' {
			if i > start {
				lines = append(lines, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func dialWorker(address string) (pb.WorkerServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	client := pb.NewWorkerServiceClient(conn)
	return client, conn, nil
}

func assignRole(client pb.WorkerServiceClient, isMapper bool, reducers []*pb.ReducerInfo, totalMappers int32, intervalStart, intervalEnd int64) error {
	_, err := client.AssignRole(context.Background(), &pb.AssignRoleRequest{
		IsMapper:      isMapper,
		Reducers:      reducers,
		TotalMappers:  totalMappers,
		IntervalStart: intervalStart,
		IntervalEnd:   intervalEnd,
	})
	return err
}

func sendChunk(client pb.WorkerServiceClient, values []int64) error {
	_, err := client.SendChunk(context.Background(), &pb.SendChunkRequest{
		Values: values,
	})
	return err
}

func assignMapper(addr string, reducerInfos []*pb.ReducerInfo) {
	client, conn, err := dialWorker(addr)
	if err != nil {
		log.Fatalf("Failed to connect to mapper %s: %v", addr, err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}()
	err = assignRole(client, true, reducerInfos, 0, 0, 0)
	if err != nil {
		log.Fatalf("Failed to assign mapper role: %v", err)
	}
	log.Printf("Assigned mapper role to %s", addr)
}

func assignReducer(addr string, cfg *Config, interval [2]int64) {
	client, conn, err := dialWorker(addr)
	if err != nil {
		log.Fatalf("Failed to connect to reducer %s: %v", addr, err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}()
	err = assignRole(client, false, nil, int32(cfg.Mappers), interval[0], interval[1])
	if err != nil {
		log.Fatalf("Failed to assign reducer role: %v", err)
	}
	log.Printf("Assigned reducer role to %s (interval [%d, %d))", addr, interval[0], interval[1])
}

func RunMaster(configPath, inputPath string) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if len(cfg.Workers) < (cfg.Mappers + cfg.Reducers) {
		log.Fatalf("Not enough workers in config for given number of mappers and reducers")
	}

	allValues, err := readInput(inputPath)
	if err != nil {
		log.Fatalf("Failed to read input: %v", err)
	}

	if len(allValues) == 0 {
		log.Fatalf("No input data provided.")
	}
	// find min and max values from input data
	var minVal, maxVal int64
	minVal = allValues[0]
	maxVal = allValues[0]
	for _, v := range allValues {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	// calculate range size for each reducer
	rangeSize := (maxVal - minVal) / int64(cfg.Reducers)
	if rangeSize == 0 {
		rangeSize = 1
	}

	// calculate intervals for each reducer
	intervals := make([][2]int64, cfg.Reducers)
	for i := 0; i < cfg.Reducers; i++ {
		start := minVal + int64(i)*rangeSize
		end := start + rangeSize
		if i == cfg.Reducers-1 {
			end = maxVal + 1
		}
		intervals[i] = [2]int64{start, end}
	}

	// slice of addresses of mappers and reducers from workers addresses list
	mapperAddrs := cfg.Workers[:cfg.Mappers]
	reducerAddrs := cfg.Workers[cfg.Mappers : cfg.Mappers+cfg.Reducers]

	// create reducer info for each reducer
	var reducerInfos []*pb.ReducerInfo
	for i, addr := range reducerAddrs {
		ri := &pb.ReducerInfo{
			Address:       addr,
			IntervalStart: intervals[i][0],
			IntervalEnd:   intervals[i][1],
		}
		reducerInfos = append(reducerInfos, ri)
	}

	// Assign roles to workers
	// Mappers
	for _, addr := range mapperAddrs {
		assignMapper(addr, reducerInfos)
	}

	// Reducers
	for i, addr := range reducerAddrs {
		assignReducer(addr, cfg, intervals[i])
	}

	// Split input into M chunks
	m := cfg.Mappers
	//ceil division to avoid empty chunks
	chunkSize := (len(allValues) + m - 1) / m

	for i, addr := range mapperAddrs {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(allValues) {
			end = len(allValues)
		}
		chunk := allValues[start:end]
		client, conn, err := dialWorker(addr)
		if err != nil {
			log.Fatalf("Failed to send chunk to mapper %s: %v", addr, err)
		}
		err = sendChunk(client, chunk)
		if err != nil {
			log.Fatalf("Failed to send chunk to mapper: %v", err)
		}
		log.Printf("Sent %d values to mapper %s", len(chunk), addr)
		if err := conn.Close(); err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}

	// The master does not wait for final outputs.
	// Mappers will notify reducers directly and reducers will write their final outputs.
	// Master is done here.
	log.Printf("Master finished distributing tasks.")
}
