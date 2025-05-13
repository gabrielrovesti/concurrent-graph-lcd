package main

import (
	"fmt"
	"sync"
	"time"
)

// Graph representation
type Graph struct {
	Adjacency map[int][]int
	Vertices  int
	Edges     int
	mutex     sync.RWMutex
}

// Create a new graph
func NewGraph(vertices int) *Graph {
	return &Graph{
		Adjacency: make(map[int][]int),
		Vertices:  vertices,
		Edges:     0,
	}
}

// Add an edge to the graph
func (g *Graph) AddEdge(src, dest int) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	
	if _, exists := g.Adjacency[src]; !exists {
		g.Adjacency[src] = []int{}
	}
	
	g.Adjacency[src] = append(g.Adjacency[src], dest)
	g.Edges++
}

// Get neighbors of a vertex
func (g *Graph) GetNeighbors(vertex int) []int {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	
	if neighbors, exists := g.Adjacency[vertex]; exists {
		return neighbors
	}
	return []int{}
}

// Worker function for BFS
func bfsWorker(id int, tasks <-chan int, results chan<- map[int]int, g *Graph, wg *sync.WaitGroup) {
	defer wg.Done()
	
	for start := range tasks {
		// BFS implementation
		distances := make(map[int]int)
		visited := make(map[int]bool)
		queue := []int{start}
		distances[start] = 0
		visited[start] = true
		
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			
			neighbors := g.GetNeighbors(current)
			
			for _, neighbor := range neighbors {
				if !visited[neighbor] {
					visited[neighbor] = true
					distances[neighbor] = distances[current] + 1
					queue = append(queue, neighbor)
				}
			}
		}
		
		results <- distances
	}
}

// Parallel BFS implementation
func (g *Graph) ParallelBFS(startVertices []int, numWorkers int) map[int]map[int]int {
	tasks := make(chan int, len(startVertices))
	results := make(chan map[int]int, len(startVertices))
	var wg sync.WaitGroup
	
	// Create worker pool
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go bfsWorker(i, tasks, results, g, &wg)
	}
	
	// Distribute tasks
	for _, v := range startVertices {
		tasks <- v
	}
	close(tasks)
	
	// Wait for completion in a separate goroutine
	go func() {
		wg.Wait()
		close(results)
	}()
	
	// Collect results
	allDistances := make(map[int]map[int]int)
	for distances := range results {
		for start, _ := range distances {
			if _, exists := allDistances[start]; !exists {
				allDistances[start] = make(map[int]int)
			}
			allDistances[start] = distances
			break // We only need the start vertex
		}
	}
	
	return allDistances
}

// Worker for PageRank calculation
func pageRankWorker(id int, vertices <-chan int, 
                    oldRank []float64, newRank []float64, 
                    g *Graph, dampingFactor float64, 
                    wg *sync.WaitGroup) {
	defer wg.Done()
	
	for v := range vertices {
		sum := 0.0
		
		g.mutex.RLock()
		// Find all vertices that link to v
		for src, dests := range g.Adjacency {
			outDegree := len(dests)
			if outDegree == 0 {
				continue
			}
			
			for _, dest := range dests {
				if dest == v {
					sum += oldRank[src] / float64(outDegree)
				}
			}
		}
		g.mutex.RUnlock()
		
		newRank[v] = (1-dampingFactor)/float64(g.Vertices) + dampingFactor*sum
	}
}

// Parallel PageRank implementation
func (g *Graph) ParallelPageRank(iterations, numWorkers int) []float64 {
	dampingFactor := 0.85
	n := g.Vertices
	
	// Initialize ranks
	oldRank := make([]float64, n)
	newRank := make([]float64, n)
	for i := 0; i < n; i++ {
		oldRank[i] = 1.0 / float64(n)
	}
	
	// Execute iterations
	for iter := 0; iter < iterations; iter++ {
		tasks := make(chan int, n)
		var wg sync.WaitGroup
		
		// Create worker pool
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go pageRankWorker(i, tasks, oldRank, newRank, g, dampingFactor, &wg)
		}
		
		// Distribute tasks
		for v := 0; v < n; v++ {
			tasks <- v
		}
		close(tasks)
		
		// Wait for completion
		wg.Wait()
		
		// Swap ranks for next iteration
		oldRank, newRank = newRank, oldRank
	}
	
	return oldRank
}

// Worker for connected components
func connectedComponentsWorker(id int, vertices <-chan int, 
                               g *Graph, componentsMap *sync.Map, 
                               wg *sync.WaitGroup) {
	defer wg.Done()
	
	for vertex := range vertices {
		// Check if vertex is already in a component
		if _, exists := componentsMap.Load(vertex); exists {
			continue
		}
		
		// Run BFS to find connected component
		visited := make(map[int]bool)
		queue := []int{vertex}
		visited[vertex] = true
		componentID := id  // Use worker ID as component ID
		
		// Assign the vertex to this component
		componentsMap.Store(vertex, componentID)
		
		// Process the component
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			
			neighbors := g.GetNeighbors(current)
			
			for _, neighbor := range neighbors {
				if !visited[neighbor] {
					visited[neighbor] = true
					queue = append(queue, neighbor)
					componentsMap.Store(neighbor, componentID)
				}
			}
		}
	}
}

// Find connected components
func (g *Graph) FindConnectedComponents(numWorkers int) map[int][]int {
	// Get all vertices
	allVertices := make([]int, 0)
	g.mutex.RLock()
	for vertex := range g.Adjacency {
		allVertices = append(allVertices, vertex)
	}
	g.mutex.RUnlock()
	
	// Create channels and wait group
	tasks := make(chan int, len(allVertices))
	var wg sync.WaitGroup
	componentsMap := &sync.Map{}
	
	// Create worker pool
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go connectedComponentsWorker(i, tasks, g, componentsMap, &wg)
	}
	
	// Distribute tasks
	for _, v := range allVertices {
		tasks <- v
	}
	close(tasks)
	
	// Wait for completion
	wg.Wait()
	
	// Collect results
	components := make(map[int][]int)
	componentsMap.Range(func(key, value interface{}) bool {
		vertex := key.(int)
		componentID := value.(int)
		
		if _, exists := components[componentID]; !exists {
			components[componentID] = []int{}
		}
		components[componentID] = append(components[componentID], vertex)
		return true
	})
	
	return components
}

// Find shortest path between two vertices
func (g *Graph) ShortestPath(source, target int, numWorkers int) (int, bool) {
	// Run BFS from source
	bfsResults := g.ParallelBFS([]int{source}, numWorkers)
	
	// Check if there is a path to target
	if distances, exists := bfsResults[source]; exists {
		if distance, hasPath := distances[target]; hasPath {
			return distance, true
		}
	}
	
	return 0, false
}

// Benchmark functions
func BenchmarkBFS(g *Graph, startVertices []int, numWorkers int, iterations int) time.Duration {
	totalTime := time.Duration(0)
	
	for i := 0; i < iterations; i++ {
		startTime := time.Now()
		g.ParallelBFS(startVertices, numWorkers)
		totalTime += time.Since(startTime)
	}
	
	return totalTime / time.Duration(iterations)
}

func BenchmarkPageRank(g *Graph, iterations int, numWorkers int, runs int) time.Duration {
	totalTime := time.Duration(0)
	
	for i := 0; i < runs; i++ {
		startTime := time.Now()
		g.ParallelPageRank(iterations, numWorkers)
		totalTime += time.Since(startTime)
	}
	
	return totalTime / time.Duration(runs)
}

func BenchmarkConnectedComponents(g *Graph, numWorkers int, iterations int) time.Duration {
	totalTime := time.Duration(0)
	
	for i := 0; i < iterations; i++ {
		startTime := time.Now()
		g.FindConnectedComponents(numWorkers)
		totalTime += time.Since(startTime)
	}
	
	return totalTime / time.Duration(iterations)
}

func BenchmarkShortestPath(g *Graph, source, target int, numWorkers int, iterations int) time.Duration {
	totalTime := time.Duration(0)
	
	for i := 0; i < iterations; i++ {
		startTime := time.Now()
		g.ShortestPath(source, target, numWorkers)
		totalTime += time.Since(startTime)
	}
	
	return totalTime / time.Duration(iterations)
}