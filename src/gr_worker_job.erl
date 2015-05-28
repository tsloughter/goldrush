%% Slight variation of sidejob_worker
-module(gr_worker_job).

-behaviour(gen_server).

%% API
-export([start/2, stop/2, start_link/6, spawn_fun/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).

%%%%%%%%%%
-record(state, {id, module, mod, modstate, timer_ref, worker_ets, reporter,
                stats_enabled, usage, limit, width, last_mq=0, enqueue=0, dequeue=0}).


%% Interface
start_link(Module, Reporter, StatsEnabled, Limit, Width, Id) ->
    Name = glc:workers_name(Module, Id),
    case gen_server:start_link(?MODULE, [Name, Module, Reporter, StatsEnabled, Limit, Width, Id], []) of
        {ok, Pid} -> yes = global:re_register_name(Name, Pid),
                     {ok, Pid};
        Else      -> Else
    end.


spawn_fun(Module, Fun, Event, OnSuccess, OnFailure) ->
    {ok, Pid} = start(glc:workers_sup(Module), undefined),
    ok = gen_server:cast(Pid, {run, Module, Fun, Event, OnSuccess, OnFailure}),
    {ok, Pid}.


start(Sup, Id) ->
    supervisor:start_child(Sup, [Id]).


stop(Sup, Pid) ->
    supervisor:terminate_child(Sup, Pid).


init([Name, Module, Reporter, StatsEnabled, Limit, Width, Id]) ->
    Mod = gr_sidejob_supervisor,
    case Mod:init([Module]) of
        {ok, ModState} ->
            process_flag(trap_exit, true),
            Exports = proplists:get_value(exports, Mod:module_info()),
            Usage = case lists:member({current_usage, 1}, Exports) of
                        true ->
                            custom;
                        false ->
                            default
                    end,
            ets:insert(Name, [{usage, 0}, {full, 0}]),
            {ok, #state{module=Module, id=Id,
                        mod=gr_sidejob_supervisor,
                        modstate=ModState,
                        usage=Usage,
                        limit=Limit,
                        width=Width,
                        worker_ets=Name,
                        reporter=Reporter,
                        stats_enabled=StatsEnabled,
                        timer_ref=schedule_tick()}};
        Else -> Else
    end.

handle_cast({run, Module, Fun, Event, Success, Failure}, State) ->
    case glc:run(Module, Fun, Event) of
        ok           -> Success(undefined);
        {ok, Result} -> Success(Result);
        Else         -> Failure(Else)
    end,
    {stop, normal, State};
handle_cast(Request, State=#state{mod=Mod,
                                  modstate=ModState}) ->
    Result = Mod:handle_cast(Request, ModState),
    {Pos, ModState2} = case Result of
                           {noreply,NewState} ->
                               {2, NewState};
                           {noreply,NewState,hibernate} ->
                               {2, NewState};
                           {noreply,NewState,_Timeout} ->
                               {2, NewState};
                           {stop,_Reason,NewState} ->
                               {3, NewState}
                       end,
    State2 = State#state{modstate=ModState2},
    State3 = update_rate(update_usage(State2)),
    Return = setelement(Pos, Result, State3),
    Return;
handle_cast(_Message, State) ->
    {noreply, State}.



handle_info('gr_worker_job_tick', State) ->
    State2 = tick(State),
    schedule_tick(),
    {noreply, State2};
handle_info(Info, State=#state{mod=Mod,
                               modstate=ModState}) ->
    Result = Mod:handle_info(Info, ModState),
    {Pos, ModState2} = case Result of
                           {noreply,NewState} ->
                               {2, NewState};
                           {noreply,NewState,hibernate} ->
                               {2, NewState};
                           {noreply,NewState,_Timeout} ->
                               {2, NewState};
                           {stop,_Reason,NewState} ->
                               {3, NewState}
                       end,
    State2 = State#state{modstate=ModState2},
    State3 = update_rate(update_usage(State2)),
    Return = setelement(Pos, Result, State3),
    Return;
handle_info(_Message, State) ->
    {noreply, State}.



handle_call(Request, From, State=#state{mod=Mod,
                                        modstate=ModState}) ->
    Result = Mod:handle_call(Request, From, ModState),
    {Pos, ModState2} = case Result of
                           {reply,_Reply,NewState} ->
                               {3, NewState};
                           {reply,_Reply,NewState,hibernate} ->
                               {3, NewState};
                           {reply,_Reply,NewState,_Timeout} ->
                               {3, NewState};
                           {noreply,NewState} ->
                               {2, NewState};
                           {noreply,NewState,hibernate} ->
                               {2, NewState};
                           {noreply,NewState,_Timeout} ->
                               {2, NewState};
                           {stop,_Reason,_Reply,NewState} ->
                               {4, NewState};
                           {stop,_Reason,NewState} ->
                               {3, NewState}
                       end,
    State2 = State#state{modstate=ModState2},
    State3 = update_rate(update_usage(State2)),
    Return = setelement(Pos, Result, State3),
    Return;
handle_call(_Request, _From, State) ->
    {reply, nop, State}.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



terminate(_Reason, #state{timer_ref=undefined}=_State) ->
    ok;
terminate(_Reason, #state{timer_ref=Ref}=_State) ->
    erlang:cancel_timer(Ref),
    ok;
terminate(_Reason, _State) ->
    ok.


tick(State=#state{stats_enabled=false}) ->
    {_In, _Out, State2} = flush_current_rate(State),
    State2;
tick(State=#state{id=Id, module=_Module, reporter=Reporter}) ->
    Usage = current_usage(State),
    {In, Out, State2} = flush_current_rate(State),
    gr_counter:job_report(Reporter, Id, Usage, In, Out),
    State2.

current_usage(#state{usage=default}) ->
    {message_queue_len, Len} = process_info(self(), message_queue_len),
    Len;
current_usage(#state{usage=custom, mod=Mod, modstate=ModState}) ->
    Mod:current_usage(ModState).

update_usage(State=#state{worker_ets=ETS, width=Width, limit=Limit}) ->
    Usage = current_usage(State),
    Full = case Usage >= (Limit div Width) of
               true ->
                   1;
               false ->
                   0
           end,
    ets:insert(ETS, [{usage, Usage},
                     {full, Full}]),
    State.

update_rate(State=#state{usage=custom}) ->
    %% Assume this is updated internally in the custom module
    State;
update_rate(State=#state{usage=default, last_mq=_LastLen}) ->
    {message_queue_len, Len} = process_info(self(), message_queue_len),
    Enqueue = Len + 1,
    %Enqueue = Len - LastLen + 1,
    Dequeue = State#state.dequeue + 1,
    State#state{enqueue=Enqueue, dequeue=Dequeue, last_mq=Len}.


flush_current_rate(State=#state{
                          usage=default,
                          enqueue=Enqueue,
                          dequeue=Dequeue}) ->
    State2 = State#state{enqueue=0, dequeue=0},
    {Enqueue, Dequeue, State2};
flush_current_rate(State=#state{usage=custom, mod=Mod, modstate=ModState}) ->
    {Enqueue, Dequeue, ModState2} = Mod:rate(ModState),
    State2 = State#state{modstate=ModState2},
    {Enqueue, Dequeue, State2}.

schedule_tick() ->
    erlang:send_after(1000, self(), 'gr_worker_job_tick').
