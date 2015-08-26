-module(deleg).
-compile(export_all).

get_main_node() ->
	'main_node@vmdomain.com'.

start() -> 
	case whereis(deleg) of
		undefined ->
			io:format("--starting deleg at ~p:~n",[node()]),
			process_flag(trap_exit,true),
			Pid = spawn_link(deleg,loop,[]),
			register(deleg,Pid),
			io:format("~p: \'deleg\' = ~p~n",[node(),whereis(deleg)]);
		_Any ->
			io:format("deleg at ~p already started~n",[node()])
	end.
	
restart() ->
	case whereis(deleg) of
		undefined ->
			io:format("deleg is not started, restarting!~n",[]),
			start();
		_Any ->
			io:format("Restarting \'deleg\'~n",[]),
			kill(),
			start()
	end.

kvs_add() ->
	rpc:call(get_main_node(),maindeleg,change_node_main_value,[node(),add]).
	
kvs_substract() ->
	rpc:call(get_main_node(),maindeleg,change_node_main_value,[node(),substract]).
	
kvs_get() ->
	rpc:call(get_main_node(),maindeleg,get_node_main_value,[node()]).
	
check_deleg() ->
	case whereis(deleg) of
		undefined ->
			start();
		_Any ->
			ok
			%io:format("Already started~n",[])		
	end.
	
kill() ->
	case whereis(deleg) of
		undefined ->
			io:format("deleg is not started yet!~n",[]);
		_Any ->
			rpc(kill,q)
	end.
	
deleg_is_alive() ->
	case whereis(deleg) of 
		undefined -> false;
		_Any -> true
	end.

inform() ->
	rpc:call(get_main_node(),maindeleg,inform,[node()]).
	
rpc(Req,Q) ->
	case whereis(deleg) of
		undefined ->
			io:format("seems like deleg has crashed, should be restarted in no time...~n",[]),
			deleg:restart();
		_Any -> ok
	end,
	deleg ! {Req, self(), Q},
	receive
		{default, Reply} -> Reply;
		{'EXIT',Pid,Why} -> io:format("Exit signal from pid: ~p,~nReason: ~p~n",[Pid,Why])
		after 3000 -> timeout
	end.

loop() ->
	receive
		{testJob, Pid, RandomNum} ->
			Pid ! (io:format("~p\treceived a job (~p ms), working on ~p job(s)~n",[node(),RandomNum,rpc:call(get_main_node(),maindeleg,get_node_main_value,[node()])+1])),
			inform(),
			kvs_add(),
			spawn(fun() -> timer(RandomNum) end),
			Pid ! {default, ok},
			loop();
%		{inform, Pid, {MainNode, Num}} ->
%			rpc:call(MainNode,io,format,["node ~p receive/finished a job, total: ~p~n",[node(),Num]]),
%			Pid ! {default, ok},
%			loop();
		{kill, _Pid, _Q} ->
			io:format("Killing deleg at ~p~n",[node()])
	end.

testJob(RandomNum) ->
	rpc(testJob, RandomNum).
	
timer(Time) ->
	receive
		_Any ->
			ok
	after Time ->
		kvs_substract(),
		inform(),
		io:format("~p\t completed its job! (~p ms, ~p) \tworking on ~p job(s)~n",[node(),Time,self(),rpc:call(get_main_node(),maindeleg,get_node_main_value,[node()])])
	end.

