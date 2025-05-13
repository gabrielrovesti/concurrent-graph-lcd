package main

import (
	"fmt"
	"math/rand"
	"time"
)

// Generate a random graph for testing
func GenerateRandomGraph(vertices int, edges int) *Graph {
	g := NewGraph(vertices)
	
	for i := 0; i < edges; i++ {
		src := rand.Intn(vertices)
		dest := rand.Intn(vertices)
		g.AddEdge(src, dest)
	}
	
	return g
}

func main() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())
	
	// Create a sample graph
	fmt.Println("Creating graph...")
	g := NewGraph(6)
	g.AddEdge(0, 1)
	g.AddEdge(0, 2)
	g.AddEdge(1, 3)
	g.AddEdge(2, 3)
	g.AddEdge(3, 4)
	g.AddEdge(3, 5)
	g.AddEdge(4, 5)
	
	// Run BFS
	fmt.Println("\nRunning parallel BFS...")
	startTime := time.Now()
	startVertices := []int{0, 3}
	bfsResults := g.ParallelBFS(startVertices, 2)
	fmt.Printf("BFS completed in %v\n", time.Since(startTime))
	
	for start, distances := range bfsResults {
		fmt.Printf("Distances from vertex %d:\n", start)
		for vertex, distance := range distances {
			fmt.Printf("  To vertex %d: %d\n", vertex, distance)
		}
	}
	
	// Run PageRank
	fmt.Println("\nRunning parallel PageRank...")
	startTime = time.Now()
	pageRanks := g.ParallelPageRank(20, 2)
	fmt.Printf("PageRank completed in %v\n", time.Since(startTime))
	
	fmt.Println("PageRank values:")
	for vertex, rank := range pageRanks {
		fmt.Printf("  Vertex %d: %.6f\n", vertex, rank)
	}
	
	// Find connected components
	fmt.Println("\nFinding connected components...")
	startTime = time.Now()
	components := g.FindConnectedComponents(2)
	fmt.Printf("Connected components found in %v\n", time.Since(startTime))
	
	fmt.Println("Connected components:")
	for componentID, vertices := range components {
		fmt.Printf("  Component %d: %v\n", componentID, vertices)
	}
	
	// Find shortest path
	fmt.Println("\nFinding shortest path...")
	startTime = time.Now()
	distance, exists := g.ShortestPath(0, 5, 2)
	fmt.Printf("Shortest path found in %v\n", time.Since(startTime))
	
	if exists {
		fmt.Printf("Distance from 0 to 5: %d\n", distance)
	} else {
		fmt.Println("No path exists from 0 to 5")
	}
	
	// Run benchmarks
	fmt.Println("\nRunning benchmarks...")
	fmt.Println("Generating larger random graph for benchmarks...")
	benchmarkGraph := GenerateRandomGraph(1000, 5000)
	
	// BFS benchmarks
	bfsTime1 := BenchmarkBFS(benchmarkGraph, []int{0, 10, 20}, 1, 5)
	bfsTime2 := BenchmarkBFS(benchmarkGraph, []int{0, 10, 20}, 2, 5)
	bfsTime4 := BenchmarkBFS(benchmarkGraph, []int{0, 10, 20}, 4, 5)
	
	fmt.Println("\nBFS Benchmark Results:")
	fmt.Printf("  1 worker: %v\n", bfsTime1)
	fmt.Printf("  2 workers: %v\n", bfsTime2)
	fmt.Printf("  4 workers: %v\n", bfsTime4)
	fmt.Printf("  Speedup (1->4): %.2fx\n", float64(bfsTime1)/float64(bfsTime4))
	
	// PageRank benchmarks
	prTime1 := BenchmarkPageRank(benchmarkGraph, 10, 1, 3)
	prTime2 := BenchmarkPageRank(benchmarkGraph, 10, 2, 3)
	prTime4 := BenchmarkPageRank(benchmarkGraph, 10, 4, 3)
	
	fmt.Println("\nPageRank Benchmark Results:")
	fmt.Printf("  1 worker: %v\n", prTime1)
	fmt.Printf("  2 workers: %v\n", prTime2)
	fmt.Printf("  4 workers: %v\n", prTime4)
	fmt.Printf("  Speedup (1->4): %.2fx\n", float64(prTime1)/float64(prTime4))
}