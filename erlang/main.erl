-module(main).
-export([run/0]).

run() ->
    io:format("Starting Concurrent Graph Processing System~n"),
    
    % Create a sample graph
    io:format("Creating graph...~n"),
    Graph = graph_processor:create_graph(6),
    graph_processor:add_edge(Graph, 0, 1),
    graph_processor:add_edge(Graph, 0, 2),
    graph_processor:add_edge(Graph, 1, 3),
    graph_processor:add_edge(Graph, 2, 3),
    graph_processor:add_edge(Graph, 3, 4),
    graph_processor:add_edge(Graph, 3, 5),
    graph_processor:add_edge(Graph, 4, 5),
    
    % Run BFS
    io:format("~nRunning parallel BFS...~n"),
    {Time1, BfsResults} = timer:tc(fun() ->
        graph_processor:parallel_bfs(Graph, [0, 3], 2)
    end),
    io:format("BFS completed in ~.6f seconds~n", [Time1/1000000]),
    
    % Display BFS results
    maps:foreach(
        fun(Start, Distances) ->
            io:format("Distances from vertex ~p:~n", [Start]),
            maps:foreach(
                fun(Vertex, Distance) ->
                    io:format("  To vertex ~p: ~p~n", [Vertex, Distance])
                end, Distances)
        end, BfsResults),
    
    % Run PageRank
    io:format("~nRunning parallel PageRank...~n"),
    {Time2, PageRanks} = timer:tc(fun() ->
        graph_processor:parallel_pagerank(Graph, 20, 2)
    end),
    io:format("PageRank completed in ~.6f seconds~n", [Time2/1000000]),
    
    % Display PageRank results
    io:format("PageRank values:~n"),
    maps:foreach(
        fun(Vertex, Rank) ->
            io:format("  Vertex ~p: ~.6f~n", [Vertex, Rank])
        end, PageRanks),
    
    % Find connected components
    io:format("~nFinding connected components...~n"),
    {Time3, Components} = timer:tc(fun() ->
        graph_processor:find_connected_components(Graph, 2)
    end),
    io:format("Connected components found in ~.6f seconds~n", [Time3/1000000]),
    
    % Display connected components
    io:format("Connected components:~n"),
    maps:foreach(
        fun(ComponentId, Vertices) ->
            io:format("  Component ~p: ~p~n", [ComponentId, Vertices])
        end, Components),
    
    % Find shortest path
    io:format("~nFinding shortest path...~n"),
    {Time4, Path} = timer:tc(fun() ->
        graph_processor:shortest_path(Graph, 0, 5, 2)
    end),
    io:format("Shortest path found in ~.6f seconds~n", [Time4/1000000]),
    io:format("Path from 0 to 5: ~p~n", [Path]),
    
    % Clean up
    graph_processor:stop(Graph),
    
    ok.