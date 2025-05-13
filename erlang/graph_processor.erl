-module(graph_processor).
-export([
    start/0, 
    create_graph/1, 
    add_edge/3, 
    get_neighbors/2,
    parallel_bfs/3, 
    parallel_pagerank/3,
    find_connected_components/2,
    shortest_path/4,
    stop/1
]).

%% Graph server process
start() ->
    spawn(fun() -> graph_server(#{}, 0, sets:new()) end).

%% Graph server loop
graph_server(Adjacency, Edges, Vertices) ->
    receive
        {create, VertexCount, Pid} ->
            Pid ! {ok, created},
            graph_server(#{}, 0, sets:new());
        
        {add_edge, Src, Dest, Pid} ->
            NewAdjacency = case maps:find(Src, Adjacency) of
                {ok, Neighbors} -> 
                    maps:put(Src, [Dest | Neighbors], Adjacency);
                error -> 
                    maps:put(Src, [Dest], Adjacency)
            end,
            
            % Update vertices set
            NewVertices = sets:add_element(Dest, sets:add_element(Src, Vertices)),
            
            Pid ! {ok, added},
            graph_server(NewAdjacency, Edges + 1, NewVertices);
        
        {get_neighbors, Vertex, Pid} ->
            Neighbors = case maps:find(Vertex, Adjacency) of
                {ok, N} -> N;
                error -> []
            end,
            Pid ! {neighbors, Neighbors},
            graph_server(Adjacency, Edges, Vertices);
        
        {get_all_edges, Pid} ->
            Pid ! {adjacency, Adjacency},
            graph_server(Adjacency, Edges, Vertices);
        
        {get_vertices_count, Pid} ->
            Pid ! {vertices, sets:size(Vertices)},
            graph_server(Adjacency, Edges, Vertices);
            
        {get_all_vertices, Pid} ->
            Pid ! {vertices_list, sets:to_list(Vertices)},
            graph_server(Adjacency, Edges, Vertices);
        
        stop ->
            ok
    end.

%% API Functions
create_graph(VertexCount) ->
    GraphPid = start(),
    GraphPid ! {create, VertexCount, self()},
    receive {ok, created} -> GraphPid end.

add_edge(GraphPid, Src, Dest) ->
    GraphPid ! {add_edge, Src, Dest, self()},
    receive {ok, added} -> ok end.

get_neighbors(GraphPid, Vertex) ->
    GraphPid ! {get_neighbors, Vertex, self()},
    receive {neighbors, Neighbors} -> Neighbors end.

stop(GraphPid) ->
    GraphPid ! stop.

%% BFS Worker
bfs_worker(GraphPid, Start, Manager) ->
    % Initialize BFS
    Queue = queue:in(Start, queue:new()),
    Distances = #{Start => 0},
    Visited = #{Start => true},
    
    % Run BFS
    Result = bfs_loop(GraphPid, Queue, Distances, Visited),
    Manager ! {bfs_result, Start, Result}.

bfs_loop(GraphPid, Queue, Distances, Visited) ->
    case queue:out(Queue) of
        {{value, Current}, NewQueue} ->
            % Get neighbors
            GraphPid ! {get_neighbors, Current, self()},
            Neighbors = receive {neighbors, N} -> N end,
            
            % Process neighbors
            {NewerQueue, NewDistances, NewVisited} = 
                process_neighbors(Neighbors, Current, NewQueue, Distances, Visited),
            
            % Continue BFS
            bfs_loop(GraphPid, NewerQueue, NewDistances, NewVisited);
        
        {empty, _} ->
            Distances
    end.

process_neighbors([], _, Queue, Distances, Visited) ->
    {Queue, Distances, Visited};
process_neighbors([Neighbor | Rest], Current, Queue, Distances, Visited) ->
    case maps:find(Neighbor, Visited) of
        {ok, true} ->
            % Already visited
            process_neighbors(Rest, Current, Queue, Distances, Visited);
        error ->
            % Not visited yet
            NewDistance = maps:get(Current, Distances) + 1,
            NewQueue = queue:in(Neighbor, Queue),
            NewDistances = maps:put(Neighbor, NewDistance, Distances),
            NewVisited = maps:put(Neighbor, true, Visited),
            process_neighbors(Rest, Current, NewQueue, NewDistances, NewVisited)
    end.

%% Parallel BFS manager
parallel_bfs(GraphPid, StartVertices, NumWorkers) ->
    Self = self(),
    
    % Start worker processes
    [spawn(fun() -> bfs_worker(GraphPid, Start, Self) end) 
     || Start <- StartVertices],
    
    % Collect results
    collect_bfs_results(length(StartVertices), #{}).

collect_bfs_results(0, Results) ->
    Results;
collect_bfs_results(Count, Results) ->
    receive
        {bfs_result, Start, Distances} ->
            collect_bfs_results(Count - 1, maps:put(Start, Distances, Results))
    end.

%% PageRank worker
pagerank_worker(GraphPid, Vertices, OldRanks, DampingFactor, TotalVertices, Manager) ->
    % Get all edges
    GraphPid ! {get_all_edges, self()},
    Adjacency = receive {adjacency, A} -> A end,
    
    % Calculate new ranks for assigned vertices
    NewRanks = calculate_new_ranks(Vertices, OldRanks, Adjacency, DampingFactor, TotalVertices),
    
    % Send results back
    Manager ! {pagerank_partial, NewRanks}.

calculate_new_ranks([], _, _, _, _) ->
    #{};
calculate_new_ranks([V | Rest], OldRanks, Adjacency, DampingFactor, TotalVertices) ->
    % Find all vertices that link to V
    Sum = maps:fold(
        fun(Src, Dests, Acc) ->
            OutDegree = length(Dests),
            case OutDegree of
                0 -> Acc;
                _ ->
                    case lists:member(V, Dests) of
                        true -> Acc + (maps:get(Src, OldRanks, 0) / OutDegree);
                        false -> Acc
                    end
            end
        end, 0, Adjacency),
    
    % Calculate new rank
    NewRank = (1 - DampingFactor) / TotalVertices + DampingFactor * Sum,
    
    % Continue with other vertices
    RestRanks = calculate_new_ranks(Rest, OldRanks, Adjacency, DampingFactor, TotalVertices),
    maps:put(V, NewRank, RestRanks).

%% Parallel PageRank manager
parallel_pagerank(GraphPid, Iterations, NumWorkers) ->
    % Get total number of vertices
    GraphPid ! {get_vertices_count, self()},
    TotalVertices = receive {vertices, Count} -> Count end,
    
    % Get all vertices
    GraphPid ! {get_all_vertices, self()},
    AllVertices = receive {vertices_list, VList} -> VList end,
    
    % Initialize ranks
    InitialRank = 1.0 / TotalVertices,
    Ranks = maps:from_list([{V, InitialRank} || V <- AllVertices]),
    
    % Run iterations
    FinalRanks = pagerank_iterations(GraphPid, Iterations, NumWorkers, Ranks, AllVertices, TotalVertices),
    FinalRanks.

pagerank_iterations(_, 0, _, Ranks, _, _) ->
    Ranks;
pagerank_iterations(GraphPid, Iterations, NumWorkers, Ranks, AllVertices, TotalVertices) ->
    Self = self(),
    DampingFactor = 0.85,
    
    % Divide vertices among workers
    WorkerVertices = split_list(AllVertices, NumWorkers),
    
    % Start worker processes
    [spawn(fun() -> 
        pagerank_worker(GraphPid, Vertices, Ranks, DampingFactor, TotalVertices, Self) 
     end) || Vertices <- WorkerVertices],
    
    % Collect results
    NewRanks = collect_pagerank_results(length(WorkerVertices), #{}),
    
    % Continue with next iteration
    pagerank_iterations(GraphPid, Iterations - 1, NumWorkers, NewRanks, AllVertices, TotalVertices).

collect_pagerank_results(0, Results) ->
    Results;
collect_pagerank_results(Count, Results) ->
    receive
        {pagerank_partial, PartialRanks} ->
            NewResults = maps:merge(Results, PartialRanks),
            collect_pagerank_results(Count - 1, NewResults)
    end.

%% Connected Components Worker
component_worker(GraphPid, VertexBatch, Manager) ->
    Results = find_components_batch(GraphPid, VertexBatch, #{}),
    Manager ! {component_results, Results}.

find_components_batch(_, [], Components) ->
    Components;
find_components_batch(GraphPid, [V | Rest], Components) ->
    % If vertex is already in a component, skip
    case vertex_in_any_component(V, Components) of
        true ->
            find_components_batch(GraphPid, Rest, Components);
        false ->
            % Run BFS to find connected component
            BfsResult = single_bfs(GraphPid, V),
            ComponentVertices = maps:keys(BfsResult),
            
            % Get next component ID
            NextId = maps:size(Components),
            
            % Add component
            NewComponents = maps:put(NextId, ComponentVertices, Components),
            
            % Continue with remaining vertices
            find_components_batch(GraphPid, Rest, NewComponents)
    end.

vertex_in_any_component(Vertex, Components) ->
    maps:fold(
        fun(_, ComponentVertices, Found) ->
            Found orelse lists:member(Vertex, ComponentVertices)
        end, false, Components).

single_bfs(GraphPid, Start) ->
    Queue = queue:in(Start, queue:new()),
    Distances = #{Start => 0},
    Visited = #{Start => true},
    
    % Run BFS
    bfs_loop(GraphPid, Queue, Distances, Visited).

%% Find Connected Components
find_connected_components(GraphPid, NumWorkers) ->
    % Get all vertices
    GraphPid ! {get_all_vertices, self()},
    AllVertices = receive {vertices_list, VList} -> VList end,
    
    % Split vertices among workers
    VertexBatches = split_list(AllVertices, NumWorkers),
    
    % Start worker processes
    Self = self(),
    [spawn(fun() -> component_worker(GraphPid, Batch, Self) end) 
     || Batch <- VertexBatches],
    
    % Collect and merge results
    AllComponents = collect_component_results(length(VertexBatches), #{}),
    
    % Merge overlapping components
    merge_overlapping_components(AllComponents).

collect_component_results(0, Results) ->
    Results;
collect_component_results(Count, Results) ->
    receive
        {component_results, ComponentsMap} ->
            MergedResults = maps:merge(Results, ComponentsMap),
            collect_component_results(Count - 1, MergedResults)
    end.

merge_overlapping_components(Components) ->
    % Implementation would detect and merge overlapping components
    % For simplicity, we're returning the components as-is
    Components.

%% Shortest Path
shortest_path(GraphPid, Source, Target, NumWorkers) ->
    % Run BFS from source
    Result = parallel_bfs(GraphPid, [Source], NumWorkers),
    
    % Extract distance to target
    case maps:find(Source, Result) of
        {ok, Distances} ->
            case maps:find(Target, Distances) of
                {ok, Distance} -> {path_exists, Distance};
                error -> no_path
            end;
        error ->
            no_path
    end.

%% Helper function to split a list into N approximately equal chunks
split_list(List, N) ->
    split_list_rec(List, length(List), N, []).

split_list_rec([], _, _, Acc) ->
    lists:reverse(Acc);
split_list_rec(List, Len, N, Acc) when N =< 0 ->
    lists:reverse([List | Acc]);
split_list_rec(List, Len, N, Acc) ->
    ChunkSize = max(1, Len div N),
    {Chunk, Rest} = lists:split(min(ChunkSize, length(List)), List),
    split_list_rec(Rest, Len - ChunkSize, N - 1, [Chunk | Acc]).