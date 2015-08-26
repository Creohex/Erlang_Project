-module(maindeleg).
-compile(export_all).

get_kvs_name() -> 'kvserv@vmdomain.com'.
	
get_basic_node_list() -> ['cluster_node_1@vmdomain.com', 'cluster_node_2@vmdomain.com', 'cluster_node_3@vmdomain.com', 'cluster_node_4@vmdomain.com'].

start() -> 
	case whereis(maindeleg) of
		undefined ->
			io:format("--starting maindeleg:~n",[]),
			process_flag(trap_exit,true),
			Pid = spawn_link(maindeleg,loop,[]),
			register(maindeleg,Pid),
			io:format("starting \'maindeleg\' (~p)~n",[whereis(maindeleg)]),
			initkvs(),
			discover_nodes(),
			%init_nodes(),
			init_node_vars();
		_Any ->
			io:format("maindeleg already started~n",[])
	end.
	
restart() ->
	case whereis(maindeleg) of
		undefined ->
			io:format("maindeleg is not started, restarting!~n",[]),
			start();
		_Any ->
			io:format("Restarting \'maindeleg\'~n",[]),
			kill(),
			sleep(1000),
			start()
		end.
	
kill() ->
	case whereis(maindeleg) of
		undefined ->
			io:format("maindeleg is not started yet!~n",[]);
		_Any ->
			rpc(kill,q)
	end.
	
initkvs() ->
	Kvsname = get_kvs_name(),
	case net_adm:ping(Kvsname) of
		pong ->
			rpc:call(Kvsname,kvs,start,[]),
			io:format("kvs is connected~n",[]);
		pang ->
			io:format("kvs is NOT connected, trying to start~n",[]),
			rpc:call(Kvsname,kvs,start,[]),
			case net_adm:ping(Kvsname) of
				pong ->
					io:format("kvs successfully started and connected!~n",[]);
				pang ->
					io:format("something went wrong... need help.(probably didn't start it in the first place..'~n",[])
			end
	end.
	
kvs_is_alive() ->
	H = get_kvs_name(),
	case net_adm:ping(H) of
		pong -> 
			case rpc:call(H,erlang,whereis,[kvs]) of
				undefined ->
					false;
				_Any ->
					true
			end;
		pang -> 
			false;
		_Any ->
			{false, "net_adm:ping error"}
	end.

maindeleg_is_alive() ->
	case whereis(maindeleg) of 
		undefined -> false;
		_Any -> true
	end.
	
get_least_busy_node() ->
	rpc(getleastbusynode, {get_kvs_name(),get_alive_node_list()}).

get_node_main_value(Node) ->
	{ok, Value} = rpc:call(get_kvs_name(),kvs,lookup,[{Node,main}]),
	Value.
	
spawn_job() ->
	RandomNumber = random:uniform(5000),
	rpc:call(get_least_busy_node(),deleg,testJob,[RandomNumber+5000]).

spawn_jobs(X) when X == 0 -> ok;
spawn_jobs(X) when X > 0 ->
	spawn_job(),
	spawn_jobs(X-1).

get_current_least_jobs() ->
	rpc(getcurrentleastjobs, {get_kvs_name(),get_least_busy_node()}).

inform(Node) ->
	rpc(inform,Node).
	
get_alive_node_list() ->
	lists:filter(fun(M) ->
		case net_adm:ping(M) of
			pong -> 
				true; 
			pang ->
				false 
		end 
	end,get_basic_node_list()).
	
status() ->
	io:format("Status:~nMaindeleg: (~p), alive?: ~p;~nkvs: (~p), alive?: ~p;~nNodes: ~p;~n",
		[whereis(maindeleg),maindeleg_is_alive(),get_kvs_name(), kvs_is_alive(),get_alive_node_list()]).

init_node_vars() ->
	rpc(initnodevars,{get_kvs_name(),get_alive_node_list()}).

init_nodes() -> %still bad to use....
	lists:foreach(fun(M) ->
		case rpc:call(M,erlang,whereis,[deleg]) of
			undefined ->
				rpc:call(M,deleg,start,[]);
			_Any -> ok
		end
	end,get_alive_node_list()).
	
change_node_main_value(Node,Action) ->
	Qlist = {Node, Action,get_kvs_name()},
	rpc(changenodevalue,Qlist).

discover_nodes() ->
	rpc(discovernodes, get_basic_node_list()).

discover_node(Node) ->
	rpc(discovernode, Node).

get_node_values() ->
	lists:foreach(fun(M) ->
		io:format("Node ~p is currently working on ~p job(s)~n",[M,get_node_main_value(M)])
	end, get_alive_node_list()).
	
clear_node_values() ->
	lists:foreach(fun(M) ->
		change_node_main_value(M,clear)
	end, get_alive_node_list()).

rpc(Req,Q) ->
	case whereis(maindeleg) of
		undefined ->
			io:format("seems like maindeleg has crashed, should be restarted in no time...~n",[]),
			maindeleg:restart();
		_Any -> ok
	end,
	maindeleg ! {Req, self(), Q},
	receive
		{default, Reply} -> Reply;
		{'EXIT',Pid,Why} -> io:format("Exit signal from pid: ~p, Reason: ~p~n",[Pid,Why])
		after 3000 -> timeout
	end.

loop() ->
	receive
		{discovernodes, Pid, Nodes} ->
			io:format("--discovering nodes:~n",[]),
			lists:foreach(fun(M) ->
						case net_adm:ping(M) of
							pong -> 
								io:format("~p is online~n",[M]);
							pang -> 
								io:format("~p is OFFline~n",[M])
						end
					end, Nodes),
			Pid ! {default, ok},
			loop();
		{discovernode, Pid, Node} ->
			case net_adm:ping(Node) of
				pong ->
					Stance = true;
				pang ->
					Stance = false
			end,
			Pid ! {default, Stance},
			loop();
%		{initnodes, Pid, Nodes} ->
%			lists:foreach(fun(M) ->
%						case rpc:call(M,erlang,whereis,[deleg]) of
%							undefined ->
%								rpc:call(M,deleg,start,[]);
%							_Any -> ok
%						end
%					end,Nodes),
%			Pid ! {default, ok},
%			loop();
		{changenodevalue, Pid, {Node,Action,Kvserv}} ->
			%io:format("received change_value command for ~p, action: ~p~n",[Node,Action]),
			OldValue = rpc:call(Kvserv,kvs,lookup,[{Node,main}]),
			case OldValue of
				undefined -> 
					io:format("kvs-main value of node ~p is undefined, trying to re-init kvs",[Node]),
					init_node_vars();
				{ok, ActualValue} ->
					case Action of
						add ->
							rpc:call(Kvserv,kvs,store,[{Node,main},ActualValue+1]);
%							io:format("kvs-main value of node ~p is now: ~p~n",[Node,ActualValue+1]);
						substract ->
							rpc:call(Kvserv,kvs,store,[{Node,main},ActualValue-1]);
%							io:format("kvs-main value of node ~p is now: ~p~n",[Node,ActualValue-1]);
						clear ->
							rpc:call(Kvserv,kvs,store,[{Node,main},0]),
							io:format("kvs-main value of node ~p is now: ~p~n",[Node,rpc:call(Kvserv,kvs,lookup,[{Node,main}])]);
						_Any ->
							io:format("Unknown action (only add or substract), nothing happens.~n",[])
					end
			end,
			Pid ! {default, ok},
			loop();
		{initnodevars, Pid, {Kvserv,Nodes}} ->
			io:format("--initiating node variables:~n",[]),
			lists:foreach(fun(M) ->
						case rpc:call(Kvserv,kvs,lookup,[{M,main}]) of
							undefined -> 
								rpc:call(Kvserv,kvs,store,[{M,main},0]),
								io:format("~p: changed kvs-main value from undefined to 0~n",[M]);
							_Any ->
								io:format("~p: kvs-main value is fine~n",[M])
						end
					end, Nodes),
			Pid ! {default, ok},
			loop();
		{getleastbusynode, Pid, {Kvserv,Nodes}} ->
			rpc:call(Kvserv,kvs,store,[leastbusycounter,undefined]),
			rpc:call(Kvserv,kvs,store,[leastbusynode,none]),
			lists:foreach(fun(M) ->
						case rpc:call(Kvserv,kvs,lookup,[leastbusycounter]) of
							{ok, undefined} ->
								{ok, FirstJobNumber} = rpc:call(Kvserv,kvs,lookup,[{M,main}]),
								rpc:call(Kvserv,kvs,store,[leastbusycounter,FirstJobNumber]),
								rpc:call(Kvserv,kvs,store,[leastbusynode,M]);
							_Any ->						
								{ok, JobNumber} = rpc:call(Kvserv,kvs,lookup,[{M,main}]),
								{ok, JobCounter} = rpc:call(Kvserv,kvs,lookup,[leastbusycounter]),
								{ok, PreviousNode} = rpc:call(Kvserv,kvs,lookup,[leastbusynode]),
								JobList = [{JobNumber,M}, {JobCounter,PreviousNode}],
								{LeastBusyCounter, LeastBusyNode} = lists:min(JobList),
								rpc:call(Kvserv,kvs,store,[leastbusynode, LeastBusyNode]),
								rpc:call(Kvserv,kvs,store,[leastbusycounter, LeastBusyCounter])
						end
					end,Nodes),
			{ok, BestNode} = rpc:call(Kvserv,kvs,lookup,[leastbusynode]),
			%{ok, BestCounter} = rpc:call(Kvserv,kvs,lookup,[leastbusycounter]),
			%io:format("Least busy node is: ~p (~p jobs at this moment)~n",[BestNode, BestCounter]),
			%get_node_values(), %FREE MEE LATER!!
			Pid ! {default, BestNode},
			loop();
		{getcurrentleastjobs, Pid, {Kvserv,LeastNode}} ->
			{ok, CurrentLeastJobs} = rpc:call(Kvserv,kvs,lookup,[{LeastNode,main}]),
			Pid ! {default, {LeastNode, CurrentLeastJobs}},
			loop();
		{inform, Pid, Node} ->
			io:format("node ~p received/completed a job, currently working on ~p jobs~n",[Node,get_node_main_value(Node)]),
			Pid ! {default, ok},
			loop();
		{kill, _, _} ->
			io:format("Killing maindeleg process, id: ~p~n",[whereis(maindeleg)])
	end.

sleep(T) ->
	receive
	after T ->
		true
	end.


