# MapReduce Sorting Project

This project implements a distributed integer values sorting system using a MapReduce-like paradigm, with a master-worker architecture and gRPC for communication.

## Overview

- **Master:**
    - Reads a configuration file (`config.yaml`) which includes the list of worker addresses and the number of mappers/reducers.
    - Assigns mapper and reducer roles to workers.
    - Assigns integer ranges to reducers and notifies them to mappers.
    - Distributes chunks of input data to the mappers.

- **Workers:**
    - **Mappers** receive chunks of unsorted integers, determine which reducer should receive each integer based on interval ranges, send the data to reducers, and then notify reducers when done.
    - **Reducers** wait for all mappers to finish sending their data, then sort the collected data and write the output to a local file.

## Project Structure

```
.
├── main.go
├── master
│   └── master.go
├── worker
│   └── worker.go
├── proto
│   ├── mapreduce.proto
│   ├── mapreduce.pb.go
│   └── mapreduce_grpc.pb.go
├── config.yaml
└── input
```

## Configuration File

`config.yaml` should list the workers and specify how many of them are mappers. For example:
```yaml
workers:
  - "localhost:50051"
  - "localhost:50052"
  - "localhost:50053"
  - "localhost:50054"
mappers: 2
```

## Input File

The `input` file should contain one integer per line, for example:
```
10
3
5
1
2
9
8
7
4
6
```

## Building the Project

From the project root:
```bash
go mod tidy
go build -o mapreduce .
```

This produces a `mapreduce` executable.

## Running the System

1. **Start the Workers**

   Open multiple terminals, one for each worker defined in `config.yaml`:
   ```bash
   ./mapreduce --mode=worker --port=:50051
   ./mapreduce --mode=worker --port=:50052
   ./mapreduce --mode=worker --port=:50053
   ./mapreduce --mode=worker --port=:50054
   ```

   Each worker will print a message indicating which port its listening on and will wait for role assignment.

2. **Run the Master**

   In a separate terminal:
   ```bash
   ./mapreduce --mode=master --config=config.yaml --input=input
   ```

3. **Processing Steps**

   The master:
    - Reads the config and input file.
    - Computes input data ranges for the reducers.
    - Assigns mappers and reducers roles, while advertising reducer ranges to mappers, and mappers total count to reducers.
    - Distributes input data chunks to the mappers.
    - Once done, the master exits.
   
   The mappers:
    - Send integers to reducers based on the reducers’ assigned intervals.
    - Notify every reducer when they've done.

   The reducers:
    - Wait for all mappers to finish sending data.
    - Sort the received data.
    - Write data to output files.

## Output Files

Each reducer produces its own sorted output file, marking it with its port number. For example:
- `reducer__XXXXX_output.txt`

These files contain the sorted integers that the reducer processed.

## Cleanup

To stop the workers, press `Ctrl+C` in their respective terminals.

## Notes

- The master does not produce a single merged file; each reducer’s output file contains a portion of the sorted data.
- Adjust `config.yaml` and `input` file as necessary for your use case.
- Ensure all workers are running before starting the master.
- For subsequent runs, workers can keep running; the master needs to be started every time.