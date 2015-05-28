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

-module(gr_counter).

-behaviour(gen_server).

%% API
-export([start_link/2, 
         list/1, lookup_element/2,
         insert_counter/3,
         update_counter/3, reset_counters/2]).

-export([new_job_stat/0, new_queue_stat/0,
         add_job_stat/4,
         job_report/5,
         queue_report/5,
         compute_job/0, compute_queue/0,
         compute_stat/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {table_id, timer_ref, stats_enabled,
                waiting=[], 
                job_worker_reports = dict:new(),
                job_usage = 0, job_rejected = 0,
                job_in = 0, job_out = 0,
                job_left_60s = 60,
                job_stats_60s = new_job_stat(),
                job_next_stats_60s = new_job_stat(),
                job_stats_total = new_job_stat(),
                queue_worker_reports = dict:new(),
                queue_usage = 0, queue_rejected = 0,
                queue_in = 0, queue_out = 0,
                queue_left_60s = 60,
                queue_stats_60s = new_queue_stat(),
                queue_next_stats_60s = new_queue_stat(),
                queue_stats_total = new_queue_stat()}).

-record(job_stat, {rejected = 0,
                   in_sum   = 0,
                   in_max   = 0,
                   out_sum  = 0,
                   out_max  = 0,
                   samples  = 0}).
-record(queue_stat, {rejected = 0,
                   in_sum   = 0,
                   in_max   = 0,
                   out_sum  = 0,
                   out_max  = 0,
                   samples  = 0}).

-define(JOB_ADD(Field, Value), Field = Stat#job_stat.Field + Value).
-define(JOB_MAX(Field, Value), Field = max(Stat#job_stat.Field, Value)).
-define(QUEUE_ADD(Field, Value), Field = Stat#queue_stat.Field + Value).
-define(QUEUE_MAX(Field, Value), Field = max(Stat#queue_stat.Field, Value)).

%%%===================================================================
%%% API
%%%===================================================================
list(Server) ->
    case (catch gen_server:call(Server, list)) of
        {'EXIT', _Reason} ->
            list(gr_manager:wait_for_pid(Server));
        Else -> Else
    end.

lookup_element(Server, Term) ->
    case (catch gen_server:call(Server, {lookup_element, Term})) of
        {'EXIT', _Reason} ->
            lookup_element(gr_manager:wait_for_pid(Server), Term);
        Else -> Else
    end.





job_report(Server, Id, Usage, In, Out) when is_atom(Server) ->
    case whereis(Server) of
        undefined -> 
            job_report(gr_manager:wait_for_pid(Server), Id, Usage, In, Out);
        Pid -> 
            case erlang:is_process_alive(Pid) of
                true ->
                    job_report(Pid, Id, Usage, In, Out);
                false ->
                    ServerPid = gr_manager:wait_for_pid(Server),
                    job_report(ServerPid, Id, Usage, In, Out)
            end
    end;
job_report(Server, Id, Usage, In, Out) when is_pid(Server) ->
    gen_server:cast(Server, {job_report, {Id, Usage, In, Out}}).



queue_report(Server, Module, Usage, In, Out) when is_atom(Server) ->
    case whereis(Server) of
        undefined -> 
            queue_report(gr_manager:wait_for_pid(Server), Module, Usage, In, Out);
        Pid -> 
            case erlang:is_process_alive(Pid) of
                true ->
                    queue_report(Pid, Module, Usage, In, Out);
                false ->
                    ServerPid = gr_manager:wait_for_pid(Server),
                    queue_report(ServerPid, Module, Usage, In, Out)
            end
    end;
queue_report(Server, Module, Usage, In, Out) when is_pid(Server) ->
    gen_server:cast(Server, {queue_report, {Module, Usage, In, Out}}).












insert_counter(Server, Counter, Value) when is_atom(Server) ->
    case whereis(Server) of
        undefined -> 
            insert_counter(gr_manager:wait_for_pid(Server), Counter, Value);
        Pid -> 
            case erlang:is_process_alive(Pid) of
                true ->
                    insert_counter(Pid, Counter, Value);
                false ->
                    ServerPid = gr_manager:wait_for_pid(Server),
                    insert_counter(ServerPid, Counter, Value)
            end
    end;
insert_counter(Server, Counter, Value) when is_pid(Server) ->
    case (catch gen_server:call(Server, {insert_counter, Counter, Value})) of
        {'EXIT', _Reason} ->
            insert_counter(gr_manager:wait_for_pid(Server), Counter, Value);
        Else -> Else
    end.


update_counter(Server, Counter, Value) when is_atom(Server) ->
    case whereis(Server) of
        undefined -> 
            update_counter(gr_manager:wait_for_pid(Server), Counter, Value);
        Pid -> 
            case erlang:is_process_alive(Pid) of
                true ->
                    update_counter(Pid, Counter, Value);
                false ->
                    ServerPid = gr_manager:wait_for_pid(Server),
                    update_counter(ServerPid, Counter, Value)
            end
    end;
update_counter(Server, Counter, Value) when is_pid(Server) ->
    gen_server:cast(Server, {update, Counter, Value}).

reset_counters(Server, Counter) ->
    case (catch gen_server:call(Server, {reset_counters, Counter})) of
        {'EXIT', _Reason} ->
            reset_counters(gr_manager:wait_for_pid(Server), Counter);
        Else -> Else
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link(Name) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name, StatsEnabled) ->
    gen_server:start_link({local, Name}, ?MODULE, [StatsEnabled], []).

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
init([StatsEnabled]) ->
    State = case StatsEnabled of
        true  -> #state{timer_ref=schedule_tick(), stats_enabled=StatsEnabled};
        false -> #state{stats_enabled=StatsEnabled}
    end,
    {ok, State}.

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
handle_call(list=Call, From, State) ->
    TableId = State#state.table_id,
    Waiting = State#state.waiting,
    case TableId of
        undefined -> {noreply, State#state{waiting=[{Call, From}|Waiting]}};
        _ -> {reply, lists:sort(handle_list(TableId)), State}
    end;
handle_call({lookup_element, Term}=Call, From, State) ->
    TableId = State#state.table_id,
    Waiting = State#state.waiting,
    case TableId of
        undefined -> {noreply, State#state{waiting=[{Call, From}|Waiting]}};
        _ -> {reply, handle_lookup_element(TableId, Term), State}
    end;
handle_call({insert_counter, Counter, Value}, From, State) ->
    Term = [{Counter, Value}],
    Call = {insert, Term},
    TableId = State#state.table_id,
    Waiting = State#state.waiting,
    case TableId of
        undefined -> {noreply, State#state{waiting=[{Call, From}|Waiting]}};
        _ -> {reply, handle_insert(TableId, Term), State}
    end;
handle_call({reset_counters, Counter}, From, State) ->
    Term = case Counter of
        _ when is_list(Counter) -> 
            [{Item, 0} || Item <- Counter];
        _ when is_atom(Counter) -> 
            [{Counter, 0}]
    end,
    Call = {insert, Term},
    TableId = State#state.table_id,
    Waiting = State#state.waiting,
    case TableId of
        undefined -> {noreply, State#state{waiting=[{Call, From}|Waiting]}};
        _ -> {reply, handle_insert(TableId, Term), State}
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
handle_cast({queue_report, Term}, 
            #state{table_id=_TableId, waiting=_Waiting, 
                   queue_worker_reports=Reports} = State) ->
    {noreply, State#state{queue_worker_reports=handle_report(Reports, Term)}};
handle_cast({job_report, Term}, 
            #state{table_id=_TableId, waiting=_Waiting, 
                   job_worker_reports=Reports} = State) ->
    {noreply, State#state{job_worker_reports=handle_report(Reports, Term)}};
handle_cast({update, Counter, Value}=Call, State) ->
    TableId = State#state.table_id,
    Waiting = State#state.waiting,
    State2 = case TableId of
        undefined -> State#state{waiting=[Call|Waiting]};
        _ -> _ = handle_update_counter(TableId, Counter, Value), 
             State
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
handle_info({'ETS-TRANSFER', TableId, _Pid, _Data}, State) ->
    _ = [ gen_server:reply(From, perform_call(TableId, Call)) 
      || {Call, From} <- State#state.waiting ],
    _ = [ handle_update_counter(TableId, Counter, Value) 
      || {update, Counter, Value} <- State#state.waiting ],

    {noreply, State#state{table_id=TableId, waiting=[]}};
handle_info('gr_counter_tick', State) ->
    State2 = tick(State),
    schedule_tick(),
    {noreply, State2};
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
terminate(_Reason, _State) ->
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
        {insert, Term} ->
            handle_insert(TableId, Term);
        {lookup_element, Term} ->
            handle_lookup_element(TableId, Term)
    end.

handle_list(TableId) ->
    ets:tab2list(TableId).

handle_update_counter(TableId, Counter, Value) ->
    ets:update_counter(TableId, Counter, Value).

handle_insert(TableId, Term) ->
    ets:insert(TableId, Term).

handle_lookup_element(TableId, Term) ->
    ets:lookup_element(TableId, Term, 2).

handle_report(Reports, {Id, UsageVal, InVal, OutVal} = _Term) ->
    dict:store(Id, {UsageVal, InVal, OutVal}, Reports).


new_job_stat() -> #job_stat{}.
new_queue_stat() -> #queue_stat{}.

add_job_stat(Rejected, In, Out, Stat) ->
    Stat#job_stat{?JOB_ADD(rejected, Rejected),
              ?JOB_ADD(in_sum, In),
              ?JOB_ADD(out_sum, Out),
              ?JOB_ADD(samples, 1),
              ?JOB_MAX(in_max, In),
              ?JOB_MAX(out_max, Out)}.

add_queue_stat(Rejected, In, Out, Stat) ->
    Stat#queue_stat{?QUEUE_ADD(rejected, Rejected),
                  ?QUEUE_ADD(in_sum, In),
                  ?QUEUE_ADD(out_sum, Out),
                  ?QUEUE_ADD(samples, 1),
                  ?QUEUE_MAX(in_max, In),
                  ?QUEUE_MAX(out_max, Out)}.

compute_stat(#job_stat{rejected=Rejected, in_sum=InSum, in_max=InMax,
              out_sum=OutSum, out_max=OutMax, samples=Samples}) ->
    compute_stat_(Rejected, InSum, InMax, OutSum, OutMax, Samples);
compute_stat(#queue_stat{rejected=Rejected, in_sum=InSum, in_max=InMax,
              out_sum=OutSum, out_max=OutMax, samples=Samples}) ->
    compute_stat_(Rejected, InSum, InMax, OutSum, OutMax, Samples).

compute_stat_(Rejected, InSum, InMax, OutSum, OutMax, Samples) ->
    InAvg = InSum div max(1,Samples),
    OutAvg = OutSum div max(1,Samples),
    {InSum, Rejected, InAvg, InMax, OutAvg, OutMax}.


schedule_tick() ->
    erlang:send_after(1000, self(), 'gr_counter_tick').

%% Aggregate all reported worker stats into unified stat report for
%% this resource
tick(State=#state{stats_enabled=false}) -> 
    State;
tick(State=#state{table_id=TableId,
                  job_left_60s=JobLeft60,
                  job_next_stats_60s=JobNext60,
                  job_stats_total=JobTotal,
                  queue_left_60s=QueueLeft60,
                  queue_next_stats_60s=QueueNext60,
                  queue_stats_total=QueueTotal,
                 stats_enabled=_StatsEnabled}) ->
    {JobUsage, JobIn, JobOut} = combine_job_reports(State),
    {QueueUsage, QueueIn, QueueOut} = combine_queue_reports(State),

    JobRejected = ets:update_counter(TableId, job_reject, 0),
    ets:update_counter(TableId, job_reject, {2,-JobRejected,0,0}),

    QueueRejected = ets:update_counter(TableId, queue_reject, 0),
    ets:update_counter(TableId, queue_reject, {2,-QueueRejected,0,0}),
    handle_update_counter(TableId, queue_output, QueueOut), 

    NewJobNext60 = add_job_stat(JobRejected, JobIn, JobOut, JobNext60),
    NewJobTotal = add_job_stat(JobRejected, JobIn, JobOut, JobTotal),
    NewQueueNext60 = add_queue_stat(QueueRejected, QueueIn, QueueOut, QueueNext60),
    NewQueueTotal = add_queue_stat(QueueRejected, QueueIn, QueueOut, QueueTotal),

    State2 = State#state{job_usage=JobUsage,
                         job_rejected=JobRejected,
                         job_in=JobIn,
                         job_out=JobOut,
                         job_next_stats_60s=NewJobNext60,
                         job_stats_total=NewJobTotal, 
                         queue_usage=QueueUsage,
                         queue_rejected=QueueRejected,
                         queue_in=QueueIn,
                         queue_out=QueueOut,
                         queue_next_stats_60s=NewQueueNext60,
                         queue_stats_total=NewQueueTotal},

    State3 = case JobLeft60 of
                 0 ->
                     State2#state{job_left_60s=59,
                                  job_stats_60s=NewJobNext60,
                                  job_next_stats_60s=new_job_stat()};
                 _ ->
                     State2#state{job_left_60s=JobLeft60-1}
             end,
    State4 = case QueueLeft60 of
                 0 ->
                     State3#state{queue_left_60s=59,
                                  queue_stats_60s=NewQueueNext60,
                                  queue_next_stats_60s=new_queue_stat()};
                 _ ->
                     State3#state{queue_left_60s=QueueLeft60-1}
             end,

    true = ets:insert(TableId, [{job_usage, JobUsage},
                                {job_stats, compute_job(State4)},
                                {queue_usage, QueueUsage},
                                {queue_stats, compute_queue(State4)}]),

    State4.

combine_job_reports(#state{job_worker_reports=Reports}) ->
    dict:fold(fun(_, {Usage, In, Out}, {UsageAcc, InAcc, OutAcc}) ->
                      {UsageAcc + Usage, InAcc + In, OutAcc + Out}
              end, {0,0,0}, Reports).
combine_queue_reports(#state{queue_worker_reports=Reports}) ->
    dict:fold(fun(_, {Usage, In, Out}, {UsageAcc, InAcc, OutAcc}) ->
                      {UsageAcc + Usage, InAcc + In, OutAcc + Out}
              end, {0,0,0}, Reports).

compute_job() ->
    compute_job(#state{}).

compute_job(#state{job_usage=Usage, job_rejected=Rejected, job_in=In, job_out=Out,
               job_stats_60s=Stats60s, job_stats_total=StatsTotal}) ->
    {Usage60, Rejected60, InAvg60, InMax60, OutAvg60, OutMax60} =
        compute_stat(Stats60s),

    {UsageTot, RejectedTot, InAvgTot, InMaxTot, OutAvgTot, OutMaxTot} =
        compute_stat(StatsTotal),

    [{usage, Usage},
     {rejected, Rejected},
     {in_rate, In},
     {out_rate, Out},
     {usage_60s, Usage60},
     {rejected_60s, Rejected60},
     {avg_in_rate_60s, InAvg60},
     {max_in_rate_60s, InMax60},
     {avg_out_rate_60s, OutAvg60},
     {max_out_rate_60s, OutMax60},
     {usage_total, UsageTot},
     {rejected_total, RejectedTot},
     {avg_in_rate_total, InAvgTot},
     {max_in_rate_total, InMaxTot},
     {avg_out_rate_total, OutAvgTot},
     {max_out_rate_total, OutMaxTot}].


compute_queue() ->
    compute_queue(#state{}).

compute_queue(#state{queue_usage=Usage, queue_rejected=Rejected, queue_in=In, queue_out=Out,
               queue_stats_60s=Stats60s, queue_stats_total=StatsTotal}) ->
    {Usage60, Rejected60, InAvg60, InMax60, OutAvg60, OutMax60} =
        compute_stat(Stats60s),

    {UsageTot, RejectedTot, InAvgTot, InMaxTot, OutAvgTot, OutMaxTot} =
        compute_stat(StatsTotal),

    [{usage, Usage},
     {rejected, Rejected},
     {in_rate, In},
     {out_rate, Out},
     {usage_60s, Usage60},
     {rejected_60s, Rejected60},
     {avg_in_rate_60s, InAvg60},
     {max_in_rate_60s, InMax60},
     {avg_out_rate_60s, OutAvg60},
     {max_out_rate_60s, OutMax60},
     {usage_total, UsageTot},
     {rejected_total, RejectedTot},
     {avg_in_rate_total, InAvgTot},
     {max_in_rate_total, InMaxTot},
     {avg_out_rate_total, OutAvgTot},
     {max_out_rate_total, OutMaxTot}].
