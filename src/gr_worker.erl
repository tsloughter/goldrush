%% Copyright (c) 2013, Pedram Nimreezi <deadzen@deadzen.com>
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

-module(gr_worker).

-behaviour(gen_server).

%% API
-export([start_link/8, 
         list/1, insert_queue/4, batch_queue/3,
         delete/2, lookup/2, lookup_element/2,
         info/1, info_size/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include_lib("stdlib/include/qlc.hrl").
-record(state, {module, table_id, timer_ref, worker_ets, stats_enabled, 
                batch_cursor, batch_limit, batch_delay, batch_results=[], 
                jobs_linearized, last_ts, reporter, queue_limit, full, 
                sequence=0, enqueue=0, dequeue=0, waiting=[]}).

-define(ATTEMPTS, 1).

%%%===================================================================
%%% API
%%%===================================================================
list(Server) ->
    case (catch gen_server:call(Server, list, infinity)) of
        {'EXIT', _Reason} ->
            list(gr_manager:wait_for_pid(Server));
        Else -> Else
    end.

info_size(Server) ->
    case (catch gen_server:call(Server, info_size, infinity)) of
        {'EXIT', _Reason} ->
            info_size(gr_manager:wait_for_pid(Server));
        Else -> Else
    end.


delete(Server, Term) ->
    case (catch gen_server:call(Server, {delete, Term}, infinity)) of
        {'EXIT', _Reason} ->
            delete(gr_manager:wait_for_pid(Server), Term);
        Else -> Else
    end.

batch_queue(Server, Module, Size) when is_atom(Server) ->
    case whereis(Server) of
        undefined -> 
            batch_queue(gr_manager:wait_for_pid(Server), Module, Size);
        Pid -> 
            case erlang:is_process_alive(Pid) of
                true ->
                    batch_queue(Pid, Module, Size);
                false ->
                    ServerPid = gr_manager:wait_for_pid(Server),
                    batch_queue(ServerPid, Module, Size)
            end
    end;
batch_queue(Server, Module, Size) when is_pid(Server) ->
    gen_server:cast(Server, {batch_queue, {Server, Module, Size}}).



insert_queue(Server, Module, {K, {V, A}} = Term, Evt) when is_integer(A) ->
    case (catch gen_server:call(Server, {insert, {Module, {K, {V, A, Evt}}}}, infinity)) of
        {'EXIT', _Reason} ->
            insert_queue(gr_manager:wait_for_pid(Server), Module, Term, Evt);
        Else -> Else
    end;
insert_queue(Server, Module, {K, V} = Term, Evt) ->
    case (catch gen_server:call(Server, {insert, {Module, {K, {V, ?ATTEMPTS, Evt}}}}, infinity)) of
        {'EXIT', _Reason} ->
            insert_queue(gr_manager:wait_for_pid(Server), Module, Term, Evt);
        Else -> Else
    end.

lookup(Server, Term) ->
    case (catch gen_server:call(Server, {lookup, Term}, infinity)) of
        {'EXIT', _Reason} ->
            lookup(gr_manager:wait_for_pid(Server), Term);
        Else -> Else
    end.

lookup_element(Server, Term) ->
    case (catch gen_server:call(Server, {lookup_element, Term}, infinity)) of
        {'EXIT', _Reason} ->
            lookup_element(gr_manager:wait_for_pid(Server), Term);
        Else -> Else
    end.

info(Server) ->
    case (catch gen_server:call(Server, info, infinity)) of
        {'EXIT', _Reason} ->
            info(gr_manager:wait_for_pid(Server));
        Else -> Else
    end.


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link(Name) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name, Module, Counts, StatsEnabled, JobsLinearized, 
           QueueLimit, BatchLimit, BatchDelay) ->
    gen_server:start_link({local, Name}, ?MODULE, 
                          [Name, Module, Counts, StatsEnabled, JobsLinearized,
                           QueueLimit, BatchLimit, BatchDelay], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Name, Module, Reporter, StatsEnabled, JobsLinearized, 
      QueueLimit, BatchLimit, BatchDelay]) ->
    process_flag(trap_exit, true),
    {ok, #state{module=Module,
                queue_limit=QueueLimit, 
                batch_limit=BatchLimit, 
                batch_delay=BatchDelay, 
                worker_ets=Name,
                full=false,
                timer_ref=schedule_tick(),
                stats_enabled=StatsEnabled,
                jobs_linearized=JobsLinearized,
                last_ts=erlang:now(),
                reporter=Reporter}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Call, From, State) when is_atom(Call), 
                                    Call =:= info; Call =:= info_size;
                                    Call =:= list ->
    TableId = State#state.table_id,
    Waiting = State#state.waiting,
    case TableId of
        undefined -> {noreply, State#state{waiting=[{Call, From}|Waiting]}};
        _ when Call =:= list -> 
            {reply, handle_list(TableId), State};
        _ when Call =:= info -> 
            {reply, handle_info(TableId), State};
        _ when Call =:= info_size -> 
            {reply, handle_info_size(TableId), State}
    end;

handle_call({Call, Term}, From, 
            #state{module=Module, table_id=TableId, 
                   waiting=Waiting, stats_enabled=StatsEnabled,
                   reporter=Reporter, full=Full}=State) when is_atom(Call), 
                                    Call =:= insert; Call =:= lookup; 
                                    Call =:= delete; Call =:= lookup_element ->
    case TableId of
        undefined -> 
            {noreply, State#state{waiting=[{{Call, Term}, From}|Waiting]}};
        _ when Call =:= lookup -> 
            {reply, handle_lookup(TableId, Term), State};
        _ when Call =:= delete -> 
            {reply, handle_delete(TableId, Term), State};
        _ when Call =:= insert -> 
            ok = maybe_batch(State),
            {Term1, State1} = maybe_rekey(Module, Term, State),
            {Reply, State2} = case (not Full) of
                true -> {handle_insert(TableId, Term1),
                         update_enqueue_rate(State1, 1)};
                false when StatsEnabled -> 
                         gr_counter:update_counter(Reporter, queue_reject, 1),
                         {false, State1};
                false -> {false, State1} 
            end,
            {reply, Reply, State2};
        _ when Call =:= lookup_element -> 
            {reply, handle_lookup_element(TableId, Term), State}
    end;

handle_call(_Request, _From, State) ->
    Reply = {error, unhandled_message},
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({batch_queue, {_Module, _Server, Size} = Term}=Call, 
            #state{table_id=TableId, waiting=Waiting, 
                   batch_cursor=Cursor} = State) ->
    State2 = case TableId of
        undefined -> State#state{waiting=[Call|Waiting]};
        _ when Size =:= 0, Cursor =/= undefined -> 
            qlc:delete_cursor(Cursor),
            State#state{batch_cursor=undefined};
        _ -> handle_batch_queue(TableId, Term, State)
    end,
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'ETS-TRANSFER', TableId, _Pid, _Data}, #state{waiting=Waiting}=State0) ->
    _ = [ gen_server:reply(From, perform_call(TableId, Call)) 
      || {Call, From} <- Waiting, Call =/= batch_queue ],

    State = case [ Term || {batch_queue, Term} <- Waiting ] of
                [] -> State0;
                [Term|_] ->  % we care about, at most, 1 of these.
                    handle_batch_queue(TableId, Term, State0)
                
            end,
    {noreply, State#state{table_id=TableId, waiting=[]}};
handle_info('gr_worker_tick', #state{module=Module, table_id=TableId,
                                     worker_ets=Server, batch_limit=Size,
                                    queue_limit=QueueLimit}=State0) ->

    State = tick(State0),
    schedule_tick(),
    State1 = case handle_info_size(TableId) of
        TableSize when TableSize > 0, TableSize < QueueLimit ->
             batch_queue(Server, Module, Size),
             State#state{full=false};
        0 -> State#state{full=false};
        _ -> %% leave nothing behind
             batch_queue(Server, Module, Size),
             State#state{full=true}
     end,
    {noreply, State1};
handle_info({success, {Ref, {_Pid, {Key, _Val, _Attempts, _Evt}}}}, 
            #state{table_id=TableId, batch_results=Results} = State) ->
    handle_delete(TableId, Key),
    {noreply, State#state{batch_results = lists:keydelete(Ref, 1, Results)}};
handle_info({failure, {Ref, {_Pid, {Key, _Val, Attempts, _Evt}}}}, 
            #state{table_id=TableId, batch_results=Results} = State0) ->
    State = case Attempts of
        0 -> handle_delete(TableId, Key),
             State0#state{batch_results = lists:keydelete(Ref, 1, Results)};
        _ -> State0
    end,
    {noreply, State};
handle_info({'EXIT', Pid, normal}, #state{batch_cursor=Cursor} = State) when 
      Pid =:= element(1, element(2, Cursor)) ->
    {noreply, State#state{batch_cursor = undefined}};
handle_info(_Info, State) ->
    {noreply, State}.



%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{timer_ref=undefined}=_State) ->
    ok;
terminate(_Reason, #state{timer_ref=Ref}=_State) ->
    erlang:cancel_timer(Ref),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

perform_call(TableId, Call) ->
    case Call of
        list ->
            handle_list(TableId);
        info ->
            handle_info(TableId);
        info_size ->
            handle_info_size(TableId);
        {insert, Term} ->
            handle_insert(TableId, Term);
        {delete, Term} ->
            handle_delete(TableId, Term);
        {lookup, Term} ->
            handle_lookup(TableId, Term);
        {lookup_element, Term} ->
            handle_lookup_element(TableId, Term)
    end.


handle_list(TableId) ->
    ets:tab2list(TableId).

handle_info(TableId) ->
    ets:info(TableId).

handle_info_size(TableId) ->
    ets:info(TableId, size).

handle_delete(TableId, Term) ->
    ets:delete(TableId, Term).

handle_insert(TableId, {_Module, {Key, {Val, Attempts, Evt}}}) ->
    TS = os:timestamp(),
    Term = {Key, {Val, Attempts, gre:append({enqueued, TS}, Evt)}},
    ets:insert(TableId, Term).

handle_lookup(TableId, Term) ->
    ets:lookup(TableId, Term).

handle_lookup_element(TableId, Term) ->
    ets:lookup_element(TableId, Term, 2).


query_handle(TableId, JobsLinearized) ->
    QH0 = qlc:q([{K,E} || {K, {_, A, _}=E} 
                         <- ets:table(TableId), A > 0 ]),
    case JobsLinearized of 
         true -> qlc:sort(QH0, [{order, ascending}]);
         false -> QH0
    end.

handle_batch_queue(TableId, Term, #state{batch_cursor=undefined,
                                        jobs_linearized=JobsLinearized}=State) ->
    QH = query_handle(TableId, JobsLinearized),
    BatchState = State#state{ batch_cursor=qlc:cursor(QH) },
    handle_batch_queue(TableId, Term, BatchState);
handle_batch_queue(TableId, {Server, Module, Size} = _Term, 
                   #state{reporter=Reporter,
                          batch_cursor=Cursor, 
                          batch_delay=BatchDelay,
                          batch_results=BatchResults,
                         stats_enabled=StatsEnabled,
                         jobs_linearized=JobsLinearized}=State) ->
    Len = handle_info_size(TableId),
    case get_batch_queue(Cursor, Size) of
        {ok, []} when BatchResults =:= [], Len > 0 ->
            QH = query_handle(TableId, JobsLinearized),
            BatchState = case Cursor of
                undefined -> State#state{ batch_cursor=qlc:cursor(QH) };
                _ -> (catch qlc:delete_cursor(Cursor)),
                     State#state{ batch_cursor=qlc:cursor(QH) }
            end,
            ok = batch_queue(Server, Module, Size),
            BatchState;
        {ok, []} ->
            %ok = batch_queue(Server, Module, 0),
            State;

        {ok, Batch} when JobsLinearized ->
            Self = self(),
            Ref0 = make_ref(),
            spawn(fun() -> 
                Results = lists:map(fun({Key, {Val, Attempts, Evt}}) ->
                                            execute(Self, Module, Reporter, StatsEnabled, 
                                                    {Key, {Val, Attempts, Evt}}) 
                                    end, Batch), 
                Self ! {Ref0, Results},
                Sleep = 10 * BatchDelay,
                timer:sleep(Sleep),
                ok = batch_queue(Server, Module, Size)
            end),
            handle_batch_results(Ref0, State);
        {ok, Batch} ->
            Self = self(),
            Ref0 = make_ref(),
            spawn(fun() -> 
                Pids = lists:map(fun({Key, {Val, Attempts, Evt}}) ->
                    spawn_monitor(fun() -> 
                          exit(execute(Self, Module, Reporter, StatsEnabled, 
                                       {Key, {Val, Attempts, Evt}}))
                    end)
                end, Batch), 

                Results = [receive {'DOWN', Ref, _, _, Reason} -> Reason 
                           end || {_, Ref} <- Pids],

                Self ! {Ref0, Results},
                Sleep = 10 * BatchDelay,
                timer:sleep(Sleep),
                ok = batch_queue(Server, Module, Size)
          end),
          handle_batch_results(Ref0, State)
    end.



tick(State=#state{stats_enabled=false}) ->
    State;
tick(#state{module=Module, table_id=_TableId, reporter=Reporter}=State) ->
    {In, Out, State2} = flush_current_rate(State),
    Usage = message_queue_len(State),
    %Usage = current_usage(State),
    gr_counter:queue_report(Reporter, Module, Usage, In, Out),
    %ets:select_delete(TableId,[{{'$1',{'$2', '$3', '$4'}},[{'<', '$3', 1}],['true']}]),
    State2.

maybe_time_queue(_Reporter, _Event, false) -> ok;
maybe_time_queue(Reporter, Event, true=_StatsEnabled) ->
      QueueTime = timer:now_diff(gre:fetch(dequeued, Event), 
                                 gre:fetch(enqueued, Event)),
      gr_counter:update_counter(Reporter, queue_time, QueueTime),
      ok.

get_batch_queue(Cursor, Size) ->
    case qlc:next_answers(Cursor, Size) of
        [] ->
            {ok, []};
        List ->
            {ok, List}
    end.

execute(Self, Module, Reporter, StatsEnabled, {Key, {Val, Attempts, Evt}}) ->
    Obj = {Key, {Val, Attempts, Evt}},
    Now = os:timestamp(),
    QueueTime = timer:now_diff(Now, gre:fetch(enqueued, Evt)),
    Event = gre:merge(gre:make([{id, Key}, %{event_handled, false},
                                {queuetime, QueueTime}, {event_linear, true},
                      {attempt, Attempts}, {dequeued, Now}], [list]), Evt),
    ok = maybe_time_queue(Reporter, Event, StatsEnabled) ,
    SubRef = make_ref(),
    MySelf = {Self, SubRef},
    Success = fun(_) -> success(Obj, MySelf) end,
    Failure = fun(_) -> failure(Obj, MySelf) end,
    %%{SubRef, glc:run(Module, Val, Event, Success, Failure)}
    case glc:call(Module, {spawn, {glc, run, [Module, Val, Event, Success, Failure]}}, infinity) of
        {error, overload} = _Err -> 
            %Failure(Err),
            undefined;
        Pid ->
            {SubRef, {Pid, Obj}}
    end.

-spec handle_batch_results(reference(), #state{}) -> #state{}.
handle_batch_results(Ref, State) ->
    Results = receive {Ref, Res0} -> Res0 end,
    BatchResults = [ R || R <- Results, R =/= undefined],
    update_dequeue_rate(State#state{batch_results=BatchResults}, 
                        length(BatchResults)).


-spec success({binary(), fun(), non_neg_integer()}, {pid(), reference()}) -> ok.
success({Key, {Val, _Attempts, Evt}}, {Pid, Ref}) ->
    Attempts = 0,
    Obj = {Ref, {Pid, {Key, Val, Attempts, Evt}}},
    Pid ! {success, Obj},
    ok.

-spec failure({binary(), fun(), non_neg_integer()}, {pid(), reference()}) -> ok.
failure({Key, {Val, Attempts0, Evt}} = _Term, {Pid, Ref}) when Attempts0 > 0 ->
    Attempts = Attempts0 - 1,
    Obj = {Ref, {Pid, {Key, Val, Attempts, Evt}}},
    Pid ! {failure, Obj},
    ok;
failure({Key, {Val, _Attempts, Evt}}, {Pid, Ref}) ->
    Attempts = -1,
    Obj = {Ref, {Pid, {Key, Val, Attempts, Evt}}},
    Pid ! {failure, Obj},
    ok.

schedule_tick() ->
    erlang:send_after(1000, self(), 'gr_worker_tick').


message_queue_len(#state{}) ->
    {message_queue_len, Len} = process_info(self(), message_queue_len),
    Len.


update_enqueue_rate(State=#state{enqueue=Enqueue}, Len) ->
    State#state{enqueue=Enqueue + Len}.

update_dequeue_rate(State=#state{dequeue=Dequeue}, Len) ->
    State#state{dequeue=Dequeue + Len}.


flush_current_rate(State=#state{enqueue=Enqueue, dequeue=Dequeue}) ->
    State2 = State#state{enqueue=0, dequeue=0},
    {Enqueue, Dequeue, State2}.



-spec maybe_batch(#state{}) -> ok.
maybe_batch(#state{batch_cursor=Cursor, worker_ets=Server,
                   module=Module, batch_limit=Size} = _State) ->
    case is_tuple(Cursor) of
        true -> ok;
        false -> ok = batch_queue(Server, Module, Size)
    end.

-spec maybe_rekey(atom(), term(), #state{}) -> {term(), #state{}}.
maybe_rekey(Module, Term, State) ->
    JobsLinearized = State#state.jobs_linearized,
    KeyUndefined = element(1, element(2, Term)) =:= undefined,
    case (JobsLinearized or KeyUndefined) of
        true -> 
            {ok, {TS, Seq, Key}} = 
                get_next_id(State#state.last_ts, 
                            State#state.sequence),
                Val = element(2, element(2, Term)),
                {{Module, {list_to_binary(integer_to_list(Key)), Val}},
                 State#state{last_ts=TS, sequence=Seq}};
        false -> {Term, State} %% doesn't matter anymore
    end.

get_next_id(TS, Seq) ->
    case get_next_seq(TS, Seq) of
        backwards_clock ->
            erlang:error(backwards_clock, [{TS, Seq}]);
        exhausted ->
            % Retry after a millisecond
            timer:sleep(1),
            get_next_id(TS, Seq);
        {ok, Time, NewSeq} ->
            {ok, {Time, NewSeq, construct_id(Time, NewSeq)}}
    end.


get_next_seq({Megas, Secs, Micros} = Time, Seq) ->
    Now = erlang:now(),
    {NowMegas, NowSecs, NowMicros} = Now,
    if
        % Time is essentially equal at the millisecond
        Megas =:= NowMegas,
        Secs =:= NowSecs,
        NowMicros div 1000 =:= Micros div 1000 ->
            case (Seq + 1) rem 4096 of
                0 -> exhausted;
                NewSeq -> {ok, Now, NewSeq}
            end;
        % Woops, clock was moved backwards by NTP
        Now < Time ->
            backwards_clock;
        % New millisecond
        true ->
            {ok, Now, 0}
    end.


construct_id({Megas, Secs, Micros}, Seq) ->
    Millis = Micros div 1000,
    Combined = (Megas * 1000000 + Secs) * 1000 + Millis,
    <<Integer:54/integer>> = <<0:1, Combined:41/integer-unsigned, 
                               Seq:12/integer-unsigned>>, 
    Integer.

