-module(benchmark).
-export([
    run_all/0,
    benchmark_bfs/4,
    benchmark_pagerank/4,
    benchmark_connected_components/2,
    benchmark_shortest_path/4,
    generate_random_graph/2
]).

%% Run all benchmarks
run_all() ->
    % Create a test graph
    Graph = generate_random_graph(1000, 5000),
    
    % Run benchmarks
    io:format("Running BFS benchmarks~n"),
    BfsResults = [benchmark_bfs(Graph, [0, 10, 20], Workers, 5) || Workers <- [1, 2, 4, 8]],
    io:format("BFS Results: ~p~n", [BfsResults]),
    
    io:format("Running PageRank benchmarks~n"),
    PrResults = [benchmark_pagerank(Graph, 10, Workers, 3) || Workers <- [1, 2, 4, 8]],
    io:format("PageRank Results: ~p~n", [PrResults]),
    
    io:format("Running Connected Components benchmarks~n"),
    CcResults = [benchmark_connected_components(Graph, Workers) || Workers <- [1, 2, 4, 8]],
    io:format("Connected Components Results: ~p~n", [CcResults]),
    
    io:format("Running Shortest Path benchmarks~n"),
    SpResults = [benchmark_shortest_path(Graph, 0, 500, Workers) || Workers <- [1, 2, 4, 8]],
    io:format("Shortest Path Results: ~p~n", [SpResults]),
    
    % Clean up
    graph_processor:stop(Graph),
    
    ok.

%% Generate a random graph for testing
generate_random_graph(NumVertices, NumEdges) ->
    % Create graph
    Graph = graph_processor:create_graph(NumVertices),
    
    % Add random edges
    add_random_edges(Graph, NumVertices, NumEdges),
    
    Graph.

add_random_edges(_, _, 0) ->
    ok;
add_random_edges(Graph, NumVertices, NumEdges) ->
    Src = rand:uniform(NumVertices) - 1,
    Dest = rand:uniform(NumVertices) - 1,
    graph_processor:add_edge(Graph, Src, Dest),
    add_random_edges(Graph, NumVertices, NumEdges - 1).

%% Benchmark functions
benchmark_bfs(Graph, StartVertices, NumWorkers, Iterations) ->
    {Time, _} = timer:tc(fun() ->
        benchmark_function(
            fun() -> graph_processor:parallel_bfs(Graph, StartVertices, NumWorkers) end,
            Iterations
        )
    end),
    Time / 1000000. % Convert to seconds

benchmark_pagerank(Graph, Iterations, NumWorkers, Runs) ->
    {Time, _} = timer:tc(fun() ->
        benchmark_function(
            fun() -> graph_processor:parallel_pagerank(Graph, Iterations, NumWorkers) end,
            Runs
        )
    end),
    Time / 1000000.

benchmark_connected_components(Graph, NumWorkers) ->
    {Time, _} = timer:tc(fun() ->
        graph_processor:find_connected_components(Graph, NumWorkers)
    end),
    Time / 1000000.

benchmark_shortest_path(Graph, Source, Target, NumWorkers) ->
    {Time, _} = timer:tc(fun() ->
        graph_processor:shortest_path(Graph, Source, Target, NumWorkers)
    end),
    Time / 1000000.

%% Helper function to run a function multiple times and average
benchmark_function(Fun, Iterations) ->
    TotalTime = lists:sum(
        [element(1, timer:tc(Fun)) || _ <- lists:seq(1, Iterations)]
    ),
    TotalTime / Iterations.