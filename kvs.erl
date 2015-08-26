-module(kvs).
-compile(export_all).

start() -> 
	case whereis(kvs) of
	undefined ->
		io:format("starting kvs.~n",[]),
		register(kvs, spawn(fun() -> loop() end));
	_Any ->
		io:format("something tried to restart kvs, but it's already running, all is ok.~n",[])
	end.

store(Key, Value) -> rpc({store, Key, Value}).
lookup(Key) -> rpc({lookup, Key}).
kill() -> rpc(kill).

rpc(Q) ->
	%io:format("Running rpc, kvs = ~p, self = ~p, Q = ~p~n",[whereis(kvs),self(),Q]),
	kvs ! {self(), Q},
	receive
		{kvs, Reply} ->
		Reply
	end.

loop() ->
	receive
		{From, {store, Key, Value}} ->
			put(Key, {ok, Value}),
			From ! {kvs, true},
			loop();
		{From, {lookup, Key}} ->
			From ! {kvs, get(Key)},
			loop();
		{From, kill} ->
			io:format("Ending myself (from = ~p, kvs = ~p)~n",[From,whereis(kvs)]),
			From ! {kvs, true}
	end.
