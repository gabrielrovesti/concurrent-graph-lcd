# Concurrent Graph Processing System

A comparative implementation of parallel graph algorithms in Go and Erlang for the Languages for Concurrency and Distribution course at University of Padua.

## Project Overview

This project demonstrates different approaches to concurrency and distribution by implementing parallel graph algorithms in both Go and Erlang. The implementations allow for a comparison of their concurrency models, performance characteristics, and programming paradigms.

### Features

- Parallel Breadth-First Search (BFS)
- Parallel PageRank calculation
- Parallel Connected Components detection
- Shortest Path calculation
- Performance benchmarking framework

## Directory Structure

concurrent-graph-processing/
├── README.md                      # This file
├── go/                            # Go implementation
│   ├── graph.go                   # Graph implementation and algorithms
│   └── main.go                    # Main program and benchmarks
└── erlang/                        # Erlang implementation
    ├── graph_processor.erl        # Graph implementation and algorithms
    ├── benchmark.erl              # Benchmarking utilities
    └── main.erl                   # Main program

## Prerequisites

### For Go Implementation
- Go 1.16 or higher

### For Erlang Implementation
- Erlang/OTP 23 or higher
- Erlang Runtime (erts)

## Running the Project

The README contains detailed instructions for running both implementations. Here's a quick summary:

### Go Implementation:
```bash
cd go
go run graph.go main.go
```

### Erlang Implementation:
```bash
cd erlang
erlc graph_processor.erl benchmark.erl main.erl
erl -noshell -s main run -s init stop
```

This project demonstrates the different approaches to concurrency in Go and Erlang, allowing you to compare their performance and expressiveness for graph algorithms.