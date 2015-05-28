%% Copyright (c) 2012, Magnus Klaar <klaar@ninenines.eu>
%% Copyright (c) 2013-2015, Pedram Nimreezi <deadzen@deadzen.com>
%%
%% Portions of sidejob integrations are provided to you under the Apache License.
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Event filter implementation.
%%
%% An event query is constructed using the built in operators exported from
%% this module. The filtering operators are used to specify which events
%% should be included in the output of the query. The default output action
%% is to copy all events matching the input filters associated with a query
%% to the output. This makes it possible to construct and compose multiple
%% queries at runtime.
%%
%% === Examples of built in filters ===
%% ```
%% %% Select all events where 'a' exists and is greater than 0.
%% glc:gt(a, 0).
%% %% Select all events where 'a' exists and is equal to 0.
%% glc:eq(a, 0).
%% %% Select all events where 'a' exists and is less than 0.
%% glc:lt(a, 0).
%% %% Select all events where 'a' exists and is anything.
%% glc:wc(a).
%%
%% %% Select no input events. Used as black hole query.
%% glc:null(false).
%% %% Select all input events. Used as passthrough query.
%% glc:null(true).
%% '''
%%
%% === Examples of combining filters ===
%% ```
%% %% Select all events where both 'a' and 'b' exists and are greater than 0.
%% glc:all([glc:gt(a, 0), glc:gt(b, 0)]).
%% %% Select all events where 'a' or 'b' exists and are greater than 0.
%% glc:any([glc:gt(a, 0), glc:gt(b, 0)]).
%% '''
%%
%% === Handling output events ===
%%
%% Once a query has been composed it is possible to override the output action
%% with an erlang function. The function will be applied to each output event
%% from the query. The return value from the function will be ignored.
%%
%% ```
%% %% Write all input events as info reports to the error logger.
%% glc:with(glc:null(true), fun(E) ->
%%     error_logger:info_report(gre:pairs(E)) end).
%% '''
%%
-module(glc).

-export([
    compile/2,
    compile/3,
    compile/4,
    handle/2,
    get/2,
    delete/1,
    reset_counters/1,
    reset_counters/2,
    workers_sup/1,
    workers_name/1,
    workers_name/2
]).

-export([
    lt/2,
    eq/2,
    gt/2,
    wc/1,
    nf/1
]).

-export([
    all/1,
    any/1,
    null/1,
    with/2
]).

-export([
    cast/2, 
    call/2, call/3,
    run/3, run/5,
    insert_queue/4,
    favorite_worker/1,
    available_worker/1,
    job_workers/2,
    is_available/2,
    is_available/3
]).

-export([
    input/1,
    output/1,
    filter/1,
    union/1
]).

-record(module, {
    'query' :: term(),
    tables :: [{atom(), atom()}],
    qtree :: term(),
    store :: term()
}).

-define(DEFAULT_LIMIT, 10000).
-define(DEFAULT_BATCH, 1000).
-define(DEFAULT_DELAY, 1). % multiplied by 10

-spec lt(atom(), term()) -> glc_ops:op().
lt(Key, Term) ->
    glc_ops:lt(Key, Term).

-spec eq(atom(), term()) -> glc_ops:op().
eq(Key, Term) ->
    glc_ops:eq(Key, Term).

-spec gt(atom(), term()) -> glc_ops:op().
gt(Key, Term) ->
    glc_ops:gt(Key, Term).

-spec wc(atom()) -> glc_ops:op().
wc(Key) ->
    glc_ops:wc(Key).

-spec nf(atom()) -> glc_ops:op().
nf(Key) ->
    glc_ops:nf(Key).

%% @doc Filter the input using multiple filters.
%%
%% For an input to be considered valid output the all filters specified
%% in the list must hold for the input event. The list is expected to
%% be a non-empty list. If the list of filters is an empty list a `badarg'
%% error will be thrown.
-spec all([glc_ops:op()]) -> glc_ops:op().
all(Filters) ->
    glc_ops:all(Filters).


%% @doc Filter the input using one of multiple filters.
%%
%% For an input to be considered valid output on of the filters specified
%% in the list must hold for the input event. The list is expected to be
%% a non-empty list. If the list of filters is an empty list a `badarg'
%% error will be thrown.
-spec any([glc_ops:op()]) -> glc_ops:op().
any(Filters) ->
    glc_ops:any(Filters).


%% @doc Always return `true' or `false'.
-spec null(boolean()) -> glc_ops:op().
null(Result) ->
    glc_ops:null(Result).


%% @doc Apply a function to each output of a query.
%%
%% Updating the output action of a query finalizes it. Attempting
%% to use a finalized query to construct a new query will result
%% in a `badarg' error.
-spec with(glc_ops:op(), fun((gre:event()) -> term())) -> glc_ops:op().
with(Query, Action) ->
    glc_ops:with(Query, Action).


%% @doc Return a union of multiple queries.
%%
%% The union of multiple queries is the equivalent of executing multiple
%% queries separately on the same input event. The advantage is that filter
%% conditions that are common to all or some of the queries only need to
%% be tested once.
%%
%% All queries are expected to be valid and have an output action other
%% than the default which is `output'. If these expectations don't hold
%% a `badarg' error will be thrown.
-spec union([glc_ops:op()]) -> glc_ops:op().
union(Queries) ->
    glc_ops:union(Queries).


%% @doc Compile a query to a module.
%%
%% On success the module representing the query is returned. The module and
%% data associated with the query must be released using the {@link delete/1}
%% function. The name of the query module is expected to be unique.
%% The counters are reset by default, unless Reset is set to false 
-spec compile(atom(), glc_ops:op() | [glc_ops:op()]) -> {ok, atom()}.
compile(Module, Query) ->
    compile(Module, Query, [], true).

-spec compile(atom(), glc_ops:op() | [glc_ops:op()], boolean()) -> {ok, atom()}.
compile(Module, Query, Reset) when is_boolean(Reset) ->
    compile(Module, Query, [], Reset);
compile(Module, Query, []) ->
    compile(Module, Query, [], true);
compile(Module, Query, Store) when is_list(Store) ->
    compile(Module, Query, Store, true).

-spec compile(atom(), glc_ops:op() | [glc_ops:op()], [{atom(), any()}], boolean()) -> {ok, atom()}.
compile(Module, Query, Store0, Reset) ->
    Width = system_width(Store0),
    Store = [ {workers, list_to_tuple(job_workers(Module, Width))}
            | get_limits(Width, get_names(Module), Store0) ],
    {ok, ModuleData} = module_data(Module, Query, Store),
    case glc_code:compile(Module, ModuleData) of
        {ok, Module} when Reset ->
            reset_counters(Module),
            {ok, Module};
        {ok, Module} ->
            {ok, Module}
    end.

is_available(Module, Worker) when is_atom(Module) ->
    {ok, WorkerLimit} = Module:get(worker_limit),
    {ok, Workers} = Module:get(workers),
    is_available(WorkerLimit, Worker, Workers).

is_available(WorkerLimit, Worker, Workers) ->
    TableId = element(Worker+1, Workers),
    case ets:lookup_element(TableId, full, 2) of
        1 -> false;
        0 -> update_available(TableId, WorkerLimit)
    end.

update_available(TableId, WorkerLimit) ->
    Usage = ets:update_counter(TableId, usage, 1),
    update_available(TableId, WorkerLimit, Usage).
    
update_available(TableId, WorkerLimit, Usage) when Usage >= WorkerLimit ->
    ets:insert(TableId, {full, 1});
update_available(_TableId, _WorkerLimit, _Usage) ->
    true.

      
 
call(Name, Msg) ->
    call(Name, Msg, 5000).

call(Name, Msg, Timeout) ->
    case available_worker(Name) of
        {error, overload} = Error -> Error;
        {ok, Worker} ->
            WorkerPid = global:whereis_name(Worker),
            gen_server:call(WorkerPid, Msg, Timeout)
    end.

cast(Name, Msg) ->
    case available_worker(Name) of
        {error, overload} = Error -> Error;
        {ok, Worker} ->
            WorkerPid = global:whereis_name(Worker),
            gen_server:cast(WorkerPid, Msg)
    end.




-spec worker_job_name(atom(), non_neg_integer()) -> atom().
worker_job_name(Module, Worker) ->
    {ok, Workers} = Module:get(workers),
    element(Worker+1, Workers).

-spec favorite_worker(atom()) -> atom().
favorite_worker(Module) ->
    {ok, Width} = Module:get(width),
    Scheduler = erlang:system_info(scheduler_id),
    Worker = Scheduler rem Width,
    worker_job_name(Module, Worker).

-spec available_worker(atom()) -> atom().
available_worker(Module) ->
    {ok, Width} = Module:get(width),
    {ok, Workers} = Module:get(workers),
    {ok, WorkerLimit} = Module:get(worker_limit),
    {ok, StatsEnabled} = Module:get(stats_enabled),
    Counters = Module:table(counters),
    Scheduler = erlang:system_info(scheduler_id),
    Worker = Scheduler rem Width,
    case is_available(WorkerLimit, Worker, Workers) of
        true -> {ok, worker_job_name(Module, Worker)};
        false -> available(Module, Counters, StatsEnabled, Workers, Width, WorkerLimit, Worker+1, Worker)
    end.

available(_Module, _Counters, false=_StatsEnabled, _Workers, _Width, _WorkerLimit, End, End) ->
    {error, overload};
available(_Module, Counters, true=_StatsEnabled, _Workers, _Width, _WorkerLimit, End, End) ->
    gr_counter:update_counter(Counters, job_reject, 1),
    {error, overload};
available(Module, Counters, StatsEnabled, Workers, Width, WorkerLimit, X, End) ->
    Worker = X rem Width,
    case is_available(WorkerLimit, Worker, Workers) of
        false -> 
            available(Module, Counters, StatsEnabled, Workers, Width, WorkerLimit, 
                      (Worker+1) rem Width, End);
        true ->
            {ok, worker_job_name(Module, Worker)}
    end.


filter_opts(NewOpts, Opts) ->
    lists:umerge(NewOpts, lists:dropwhile(fun({X, _Y}) ->  
                                        lists:keymember(X, 1, NewOpts) 
                                end, Opts)).


get_limits(Width, NewOpts0, Opts) ->
    Limit = system_limit(Opts),
    QueueLimit = queue_limit(Opts),
    BatchLimit = batch_limit(Opts),
    BatchDelay = batch_delay(Opts),
    StatsEnabled = stats_enabled(Opts),
    JobsLinearized = jobs_linearized(Opts),
    NewOpts = [{width, Width}, {limit, Limit},
     {queue_limit, QueueLimit},
     {batch_limit, BatchLimit},
     {batch_delay, BatchDelay},
     {stats_enabled, StatsEnabled},
     {jobs_linearized, JobsLinearized},
     {worker_limit, Limit div Width}|NewOpts0],
    filter_opts(NewOpts, Opts).

get_names(Module) ->
    Params = params_name(Module),
    Counts = counts_name(Module),
    Workers = workers_name(Module),
    JobWorkers = workers_sup(Module),
    [{params_name, Params},
     {counts_name, Counts},
     {workers_name, Workers},
     {workers_sup, JobWorkers}].
     %{params_mgr_name, ManageParams}, % necessary?
     %{counts_mgr_name, ManageCounts},
     %{workers_mgr_name, ManageWorkers}].


%% @doc Handle an event using a compiled query.
%%
%% The input event is expected to have been returned from {@link gre:make/2}.
-spec handle(atom(), list({atom(), term()}) | gre:event()) -> ok.
handle(Module, Event) when is_list(Event) ->
    Module:handle(gre:make(Event, [list]));
handle(Module, Event) ->
    Module:handle(Event).

get(Module, Key) ->
    Module:get(Key).


insert_queue(Module, Id, Fun, Event) when is_list(Event) ->
    Module:insert_queue(Id, Fun, gre:make(Event, [list]));
insert_queue(Module, Id, Fun, Event) ->
    Module:insert_queue(Id, Fun, Event).

run(Module, Fun, Event) when is_list(Event) ->
    Module:run(Fun, gre:make(Event, [list]));
run(Module, Fun, Event) ->
    Module:run(Fun, Event).

run(Module, Fun, Event, Success, Failure) ->
    {Scenario, Outcome} = case run(Module, Fun, Event) of
        ok           -> {success, Success(undefined)};
        {ok, Result} -> {success, Success(Result)};
        Else         -> {failure, Failure(Else)}
    end,
    case Scenario of
        success -> {ok, Outcome};
        failure -> {error, Outcome} %% @todo: maybe do something??
    end.


%% @doc The number of input events for this query module.
-spec input(atom()) -> non_neg_integer().
input(Module) ->
    Module:info(event_input).

%% @doc The number of output events for this query module.
-spec output(atom()) -> non_neg_integer().
output(Module) ->
    Module:info(event_output).

%% @doc The number of filtered events for this query module.
-spec filter(atom()) -> non_neg_integer().
filter(Module) ->
    Module:info(event_filter).


%% @doc Release a compiled query.
%%
%% This releases all resources allocated by a compiled query. The query name
%% is expected to be associated with an existing query module. Calling this
%% function will shutdown all relevant processes and purge/delete the module.
-spec delete(atom()) -> ok.
delete(Module) ->
    Params = params_name(Module),
    Counts = counts_name(Module),
    Workers = workers_name(Module),
    JobWorkers = workers_sup(Module),
    ManageParams = manage_params_name(Module),
    ManageCounts = manage_counts_name(Module),
    ManageWorkers = manage_workers_name(Module),

    _ = [ begin 
        _ = supervisor:terminate_child(Sup, Name),
        _ = supervisor:delete_child(Sup, Name)
      end || {Sup, Name} <- 
        [{gr_manager_sup, ManageParams}, 
         {gr_manager_sup, ManageCounts},
         {gr_manager_sup, ManageWorkers},
         {gr_param_sup, Params}, {gr_counter_sup, Counts},
         {gr_worker_sup, Workers}, 
         {gr_worker_sup, JobWorkers}]
    ],

    code:soft_purge(Module),
    code:delete(Module),
    ok.

%% @doc Reset all counters
%%
%% This resets all the counters associated with a module
-spec reset_counters(atom()) -> ok.
reset_counters(Module) ->
    Module:reset_counters(all).

%% @doc Reset a specific counter
%%
%% This resets a specific counter associated with a module
-spec reset_counters(atom(), atom()) -> ok.
reset_counters(Module, Counter) ->
    Module:reset_counters(Counter).

%% @private Map a query to a module data term.
-spec module_data(atom(), term(), term()) -> {ok, #module{}}.
module_data(Module, Query, Store) ->
    %% terms in the query which are not valid arguments to the
    %% erl_syntax:abstract/1 functions are stored in ETS.
    %% the terms are only looked up once they are necessary to
    %% continue evaluation of the query.

    %% query counters are stored in a shared ETS table. this should
    %% be an optional feature. enabled by defaults to simplify tests.
    %% the abstract_tables/1 function expects a list of name-atom pairs.
    %% tables are referred to by name in the generated code. the table/1
    %% function maps names to registered processes response for those tables.
    Tables = module_tables(Module, Store),
    Query2 = glc_lib:reduce(Query),
    {ok, #module{'query'=Query, tables=Tables, qtree=Query2, store=Store}}.

%% @private Create a data managed supervised process for params, counter tables
module_tables(Module, Store) ->
    Params = params_name(Module),
    Counts = counts_name(Module),
    Workers = workers_name(Module),
    JobWorkers = workers_sup(Module),
    ManageParams = manage_params_name(Module),
    ManageCounts = manage_counts_name(Module),
    ManageWorkers = manage_workers_name(Module),
    JobState = gr_counter:compute_job(),
    QueueState = gr_counter:compute_queue(),

    StatsEnabled = stats_enabled(Store),
    JobsLinearized = jobs_linearized(Store),

    Counters = [{event_input,0}, {event_filter,0}, {event_output,0}, 
                {job_reject, 0}, {job_usage, 0},
                {job_stats, JobState},
                {job_input, 0}, {job_run,0}, 
                {job_time, 0},  {job_error, 0},

                {queue_reject, 0}, {queue_usage, 0},
                {queue_stats, QueueState},
                {queue_input, 0}, {queue_output, 0},
                {queue_time, 0}], % {queue_length, 0}],

    {_, Width} = lists:keyfind(width, 1, Store),
    {_, Limit} = lists:keyfind(limit, 1, Store),
    {_, QueueLimit} = lists:keyfind(queue_limit, 1, Store),
    {_, BatchLimit} = lists:keyfind(batch_limit, 1, Store),
    {_, BatchDelay} = lists:keyfind(batch_delay, 1, Store),
    _ = supervisor:start_child(gr_param_sup, 
        {Params, {gr_param, start_link, [Params]}, 
        transient, brutal_kill, worker, [Params]}),
    _ = supervisor:start_child(gr_counter_sup, 
        {Counts, {gr_counter, start_link, [Counts, StatsEnabled]}, 
        transient, brutal_kill, worker, [Counts]}),
    _ = supervisor:start_child(gr_worker_sup, 
        {Workers, {gr_worker, start_link, [Workers, Module, Counts, 
                                           StatsEnabled, JobsLinearized, 
                                           QueueLimit, BatchLimit, BatchDelay]}, 
        transient, brutal_kill, worker, [Workers]}),
    _ = supervisor:start_child(gr_worker_sup, 
        {JobWorkers, {gr_worker_job_sup, start_link, [JobWorkers, Module, Counts, 
                                                      StatsEnabled, Limit, Width]}, 
        transient, brutal_kill, supervisor, [JobWorkers]}),

    _ = supervisor:start_child(gr_manager_sup, 
        {ManageParams, {gr_manager, start_link, [ManageParams, Params, [],
                                                 JobsLinearized]},
        transient, brutal_kill, worker, [ManageParams]}),
    _ = supervisor:start_child(gr_manager_sup, 
        {ManageWorkers, {gr_manager, start_link, [ManageWorkers, Workers, [],
                                                  JobsLinearized]},
        transient, brutal_kill, worker, [ManageWorkers]}),
    _ = supervisor:start_child(gr_manager_sup, 
        {ManageCounts, {gr_manager, start_link, [ManageCounts, Counts, Counters, 
                                                 JobsLinearized]},
        transient, brutal_kill, worker, [ManageCounts]}),

    _Pids = [ gr_worker_job:start(JobWorkers, Id) || Id <- lists:seq(1, Width)],

    ok = workers_ready(JobWorkers, Width),

    [{params,Params}, {counters, Counts}, {workers, Workers}].

reg_name(Module, Name) ->
    list_to_atom("gr_" ++ atom_to_list(Module) ++ Name).

params_name(Module) -> reg_name(Module, "_params").
counts_name(Module) -> reg_name(Module, "_counters").
manage_params_name(Module) -> reg_name(Module, "_params_mgr").
manage_counts_name(Module) -> reg_name(Module, "_counters_mgr").

manage_workers_name(Module) -> reg_name(Module, "_workers_mgr").
workers_name(Module) -> reg_name(Module, "_workers").
workers_sup(Module) -> reg_name(Module, "_workers_sup").
workers_name(Module, Id) ->
    NodeId = integer_to_list(erlang:crc32(term_to_binary(node()))),
    reg_name(Module, "_workers_job_" ++ NodeId ++ "_" ++ integer_to_list(Id)). 

job_workers(Module, Count) -> 
    [workers_name(Module, Id) || Id <- lists:seq(1,Count)].

workers_ready(Sup, Workers) ->
    case lists:keyfind(active, 1, supervisor:count_children(Sup)) of
        {_, Workers} -> 
            ok;
        {_, _Active} -> timer:sleep(1), 
             workers_ready(Sup, Workers)
    end.


-spec queue_limit([{atom(), any()}]) -> pos_integer().
queue_limit(Opts) ->
    get_integer_setting(queue_limit, Opts, ?DEFAULT_LIMIT*25).

-spec batch_delay([{atom(), any()}]) -> pos_integer().
batch_delay(Opts) ->
    get_integer_setting(batch_delay, Opts, ?DEFAULT_DELAY).

-spec batch_limit([{atom(), any()}]) -> pos_integer().
batch_limit(Opts) ->
    get_integer_setting(batch_limit, Opts, ?DEFAULT_LIMIT).

-spec system_limit([{atom(), any()}]) -> pos_integer().
system_limit(Opts) ->
    get_integer_setting(limit, Opts, ?DEFAULT_LIMIT).

-spec system_width([{atom(), any()}]) -> pos_integer().
system_width(Opts) ->
    get_integer_setting(width, Opts, erlang:system_info(schedulers)).

-spec stats_enabled([{atom(), any()}]) -> boolean().
stats_enabled(Opts) ->
    get_boolean_setting(stats_enabled, Opts, true).

-spec jobs_linearized([{atom(), any()}]) -> boolean().
jobs_linearized(Opts) ->
    get_boolean_setting(jobs_linearized, Opts, true).

get_integer_setting(Key, Opts, Default) ->
    case lists:keyfind(Key, 1, Opts) of
           {_, Value} when is_integer(Value) -> 
               Value;
           {_, Value} -> 
               erlang:error(badarg, [Value]);
           false -> Default
    end.

get_boolean_setting(Key, Opts, Default) ->
    case lists:keyfind(Key, 1, Opts) of
           {_, Value} when is_boolean(Value) -> 
               Value;
           {_, Value} -> 
               erlang:error(badarg, [Value]);
           false -> Default
    end.



%% @todo Move comment.
%% @private Map a query to a simplified query tree term.
%%
%% The simplified query tree is used to combine multiple queries into one
%% query module. The goal of this is to reduce the filtering and dispatch
%% overhead when multiple concurrent queries are executed.
%%
%% A fixed selection condition may be used to specify a property that an event
%% must have in order to be considered part of the input stream for a query.
%%
%% For the sake of simplicity it is only possible to define selection
%% conditions using the fields present in the context and identifiers
%% of an event. The fields in the context are bound to the reserved
%% names:
%%
%% - '$n': node name
%% - '$a': application name
%% - '$p': process identifier
%% - '$t': timestamp
%% 
%%
%% If an event must be selected based on the runtime state of an event handler
%% this must be done in the body of the handler.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

setup_query(Module, Query) ->
    setup_query(Module, Query, []).

setup_query(Module, Query, Store) ->
    ?assertNot(erlang:module_loaded(Module)),
    ?assertEqual({ok, Module}, case (catch compile(Module, Query, Store)) of
        {'EXIT',_}=Error -> ?debugFmt("~p", [Error]), Error; Else -> Else end),
    ?assert(erlang:function_exported(Module, table, 1)),
    ?assert(erlang:function_exported(Module, handle, 1)),
    {compiled, Module}.

events_test_() ->
    {foreach,
        fun() ->
                error_logger:tty(false),
                application:start(syntax_tools),
                application:start(compiler),
                application:start(goldrush)
        end,
        fun(_) ->
                application:stop(goldrush),
                application:stop(compiler),
                application:stop(syntax_tools),
                error_logger:tty(true)
        end,
        [
            {"null query compiles",
                fun() ->
                    {compiled, Mod} = setup_query(testmod1, glc:null(false)),
                    ?assertError(badarg, Mod:table(noexists))
                end
            },
            {"params table exists",
                fun() ->
                    {compiled, Mod} = setup_query(testmod2, glc:null(false)),
                    ?assert(is_atom(Mod:table(params))),
                    ?assertMatch([_|_], gr_param:info(Mod:table(params)))
                end
            },
            {"null query exists",
                fun() ->
                    {compiled, Mod} = setup_query(testmod3, glc:null(false)),
                    ?assert(erlang:function_exported(Mod, info, 1)),
                    ?assertError(badarg, Mod:info(invalid)),
                    ?assertEqual({null, false}, Mod:info('query'))
                end
            },
            {"init counters test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod4, glc:null(false)),
                    ?assertEqual(0, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(event_output))
                end
            },
            {"filtered events test",
                fun() ->
                    %% If no selection condition is specified no inputs can match.
                    {compiled, Mod} = setup_query(testmod5, glc:null(false)),
                    glc:handle(Mod, gre:make([], [list])),
                    ?assertEqual(1, Mod:info(event_input)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(event_output))
                end
            },
            {"nomatch event test",
                fun() ->
                    %% If a selection condition but no body is specified the event
                    %% is expected to count as filtered out if the condition does
                    %% not hold.
                    {compiled, Mod} = setup_query(testmod6, glc:eq('$n', 'noexists@nohost')),
                    glc:handle(Mod, gre:make([{'$n', 'noexists2@nohost'}], [list])),
                    ?assertEqual(1, Mod:info(event_input)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(event_output))
                end
            },
            {"opfilter equal test",
                fun() ->
                    %% If a selection condition but no body is specified the event
                    %% counts as input to the query, but not as filtered out.
                    {compiled, Mod} = setup_query(testmod7, glc:eq('$n', 'noexists@nohost')),
                    glc:handle(Mod, gre:make([{'$n', 'noexists@nohost'}], [list])),
                    ?assertEqual(1, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(1, Mod:info(event_output))
                end
            },
            {"opfilter wildcard test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod8, glc:wc(a)),
                    glc:handle(Mod, gre:make([{b, 2}], [list])),
                    ?assertEqual(1, Mod:info(event_input)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(event_output)),
                    glc:handle(Mod, gre:make([{a, 2}], [list])),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(1, Mod:info(event_output))
                end
            },
            {"opfilter notfound test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod9, glc:nf(a)),
                    glc:handle(Mod, gre:make([{a, 2}], [list])),
                    ?assertEqual(1, Mod:info(event_input)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(event_output)),
                    glc:handle(Mod, gre:make([{b, 2}], [list])),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(1, Mod:info(event_output))
                end
            },
            {"opfilter greater than test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod10, glc:gt(a, 1)),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    ?assertEqual(1, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    glc:handle(Mod, gre:make([{'a', 0}], [list])),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(1, Mod:info(event_output))
                end
            },
            {"opfilter less than test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod11, glc:lt(a, 1)),
                    glc:handle(Mod, gre:make([{'a', 0}], [list])),
                    ?assertEqual(1, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(1, Mod:info(event_output)),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(1, Mod:info(event_output))
                end
            },
            {"allholds op test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod12,
                        glc:all([glc:eq(a, 1), glc:eq(b, 2)])),
                    glc:handle(Mod, gre:make([{'a', 1}], [list])),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(2, Mod:info(event_filter)),
                    glc:handle(Mod, gre:make([{'b', 1}], [list])),
                    glc:handle(Mod, gre:make([{'b', 2}], [list])),
                    ?assertEqual(4, Mod:info(event_input)),
                    ?assertEqual(4, Mod:info(event_filter)),
                    glc:handle(Mod, gre:make([{'a', 1},{'b', 2}], [list])),
                    ?assertEqual(5, Mod:info(event_input)),
                    ?assertEqual(4, Mod:info(event_filter)),
                    ?assertEqual(1, Mod:info(event_output))
                end
            },
            {"anyholds op test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod13,
                        glc:any([glc:eq(a, 1), glc:eq(b, 2)])),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    glc:handle(Mod, gre:make([{'b', 1}], [list])),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(2, Mod:info(event_filter)),
                    glc:handle(Mod, gre:make([{'a', 1}], [list])),
                    glc:handle(Mod, gre:make([{'b', 2}], [list])),
                    ?assertEqual(4, Mod:info(event_input)),
                    ?assertEqual(2, Mod:info(event_filter))
                end
            },
            {"with function test",
                fun() ->
                    Self = self(),
                    {compiled, Mod} = setup_query(testmod14,
                        glc:with(glc:eq(a, 1), fun(Event) -> Self ! gre:fetch(a, Event) end)),
                    glc:handle(Mod, gre:make([{a,1}], [list])),
                    ?assertEqual(1, Mod:info(event_output)),
                    ?assertEqual(1, receive Msg -> Msg after 0 -> notcalled end)
                end
            },
            {"with function storage test",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],
                    {compiled, Mod} = setup_query(testmod15a,
                        glc:with(glc:eq(a, 1), fun(Event, EStore) -> 
                           Self ! {gre:fetch(a, Event), EStore} end),
                         Store),
                    glc:handle(Mod, gre:make([{a,1}], [list])),
                    ?assertEqual(1, Mod:info(event_output)),
                    ?assertEqual(1, receive {Msg, _Store} -> Msg after 0 -> notcalled end)
                end
            },
            {"with multi-function output double-match test",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],
                    {compiled, Mod} = setup_query(testmod15b,
                          [glc:with(glc:eq(a, 1), fun(Event, _EStore) -> 
                              Self ! {a, gre:fetch(a, Event)} end),
                           glc:with(glc:eq(b, 1), fun(Event, _EStore) -> 
                              Self ! {b, gre:fetch(b, Event)} end)],
                         Store),
                    glc:handle(Mod, gre:make([{a,1}, {b, 1}], [list])),
                    ?assertEqual(2, Mod:info(event_output)),
                    ?assertEqual(a, receive {a=Msg, _Store} -> Msg after 0 -> notcalled end),
                    ?assertEqual(b, receive {b=Msg, _Store} -> Msg after 0 -> notcalled end)
                end
            },
            {"with multi-function output match test",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],

                    {compiled, Mod} = setup_query(testmod15c,
                          [glc:with(glc:eq(a, 1), fun(Event, _EStore) -> 
                              Self ! {a, gre:fetch(a, Event)} end),
                           glc:with(glc:gt(b, 1), fun(Event, _EStore) -> 
                              Self ! {b, gre:fetch(b, Event)} end)],
                         Store),
                    glc:handle(Mod, gre:make([{a,1}, {b, 1}], [list])),
                    ?assertEqual(1, Mod:info(event_output)),
                    ?assertEqual(a, receive {a=Msg, _Store} -> Msg after 0 -> notcalled end)

                end
            },
            {"delete test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod16, glc:null(false)),
                    ?assert(is_atom(Mod:table(params))),
                    ?assertMatch([_|_], gr_param:info(Mod:table(params))),
                    ?assert(is_list(code:which(Mod))),
                    ?assert(is_pid(whereis(params_name(Mod)))),
                    ?assert(is_pid(whereis(counts_name(Mod)))),
                    ?assert(is_pid(whereis(manage_params_name(Mod)))),
                    ?assert(is_pid(whereis(manage_counts_name(Mod)))),

                    glc:delete(Mod),
                    
                    ?assertEqual(non_existing, code:which(Mod)),
                    ?assertEqual(undefined, whereis(params_name(Mod))),
                    ?assertEqual(undefined, whereis(counts_name(Mod))),
                    ?assertEqual(undefined, whereis(manage_params_name(Mod))),
                    ?assertEqual(undefined, whereis(manage_counts_name(Mod)))
                end
            },
            {"reset counters test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod17,
                        glc:any([glc:eq(a, 1), glc:eq(b, 2)])),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    glc:handle(Mod, gre:make([{'b', 1}], [list])),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(2, Mod:info(event_filter)),
                    glc:handle(Mod, gre:make([{'a', 1}], [list])),
                    glc:handle(Mod, gre:make([{'b', 2}], [list])),
                    ?assertEqual(4, Mod:info(event_input)),
                    ?assertEqual(2, Mod:info(event_filter)),
                    ?assertEqual(2, Mod:info(event_output)),

                    glc:reset_counters(Mod, event_input),
                    ?assertEqual(0, Mod:info(event_input)),
                    ?assertEqual(2, Mod:info(event_filter)),
                    ?assertEqual(2, Mod:info(event_output)),
                    glc:reset_counters(Mod, event_filter),
                    ?assertEqual(0, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(2, Mod:info(event_output)),
                    glc:reset_counters(Mod),
                    ?assertEqual(0, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(event_output))
                end
            },
            {"ets data recovery test",
                fun() ->
                    Self = self(),
                    {compiled, Mod} = setup_query(testmod18,
                        glc:with(glc:eq(a, 1), fun(Event) -> Self ! gre:fetch(a, Event) end)),
                    glc:handle(Mod, gre:make([{a,1}], [list])),
                    ?assertEqual(1, Mod:info(event_output)),
                    ?assertEqual(1, receive Msg -> Msg after 0 -> notcalled end),
                    ?assertEqual(1, length(gr_param:list(Mod:table(params)))),
                    ?assertEqual(16, length(gr_param:list(Mod:table(counters)))),
                    true = exit(whereis(Mod:table(params)), kill),
                    true = exit(whereis(Mod:table(counters)), kill),
                    ?assertEqual(1, Mod:info(event_input)),
                    glc:handle(Mod, gre:make([{'a', 1}], [list])),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(2, Mod:info(event_output)),
                    ?assertEqual(1, length(gr_param:list(Mod:table(params)))),
                    ?assertEqual(16, length(gr_counter:list(Mod:table(counters))))
                end
            },
            {"run timed job test",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],
                    Runtime = 150000,
                    {compiled, Mod} = setup_query(testmod19,
                                                  glc:gt(runtime, Runtime),
                                                  Store),
                    glc:run(Mod, fun(Event, EStore) -> 
                        timer:sleep(100),
                        Self ! {gre:fetch(a, Event), EStore}
                    end, gre:make([{a,1}], [list])),
                    ?assertEqual(0, Mod:info(event_output)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(1, receive {Msg, _Store} -> Msg after 0 -> notcalled end),

                    delete(testmod19),
                    {compiled, Mod} = setup_query(testmod19,
                                                  glc:gt(runtime, Runtime),
                                                  Store),
                    glc:handle(Mod, gre:make([{'a', 1}], [list])),
                    glc:run(Mod, fun(Event, EStore) -> 
                        timer:sleep(200),
                        Self ! {gre:fetch(a, Event), EStore}
                    end, gre:make([{a,2}], [list])),
                    ?assertEqual(1, Mod:info(event_output)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(2, receive {Msg, _Store} -> Msg after 0 -> notcalled end)

                end
            },
            {"reset job counters",
                fun() ->
                    {compiled, Mod} = setup_query(testmod20,
                        glc:any([glc:eq(a, 1), glc:gt(runtime, 150000)])),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    glc:handle(Mod, gre:make([{'b', 1}], [list])),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(2, Mod:info(event_filter)),
                    glc:handle(Mod, gre:make([{'a', 1}], [list])),
                    glc:handle(Mod, gre:make([{'b', 2}], [list])),
                    ?assertEqual(4, Mod:info(event_input)),
                    ?assertEqual(3, Mod:info(event_filter)),
                    ?assertEqual(1, Mod:info(event_output)),

                    Self = self(),
                    glc:run(Mod, fun(Event, EStore) -> 
                        timer:sleep(100),
                        Self ! {gre:fetch(a, Event), EStore}
                    end, gre:make([{a,1}], [list])),
                    ?assertEqual(2, Mod:info(event_output)),
                    ?assertEqual(3, Mod:info(event_filter)),
                    ?assertEqual(1, receive {Msg, _} -> Msg after 0 -> notcalled end),

                    Msg1 = glc:run(Mod, fun(_Event, _EStore) -> 
                        timer:sleep(200),
                        {error, badtest}
                        
                    end, gre:make([{a,1}], [list])),
                    ?assertEqual(2, Mod:info(event_output)),
                    ?assertEqual(3, Mod:info(event_filter)),
                    ?assertEqual(2, Mod:info(job_input)),
                    ?assertEqual(1, Mod:info(job_error)),
                    ?assertEqual(1, Mod:info(job_run)),
                    ?assertEqual({error, badtest}, Msg1),

                    Msg2 = glc:run(Mod, fun(_Event, _EStore) -> 
                        timer:sleep(200),
                        {ok, goodtest}
                        
                    end, gre:make([{a,1}], [list])),
                    ?assertEqual(3, Mod:info(event_output)),
                    ?assertEqual(3, Mod:info(event_filter)),
                    ?assertEqual(3, Mod:info(job_input)),
                    ?assertEqual(1, Mod:info(job_error)),
                    ?assertEqual(2, Mod:info(job_run)),
                    ?assertEqual({ok, goodtest}, Msg2),


                    glc:reset_counters(Mod, event_input),
                    ?assertEqual(0, Mod:info(event_input)),
                    ?assertEqual(3, Mod:info(event_filter)),
                    ?assertEqual(3, Mod:info(event_output)),
                    ?assertEqual(3, Mod:info(job_input)),
                    ?assertEqual(1, Mod:info(job_error)),
                    ?assertEqual(2, Mod:info(job_run)),
                    glc:reset_counters(Mod, event_filter),
                    ?assertEqual(0, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(3, Mod:info(event_output)),
                    ?assertEqual(3, Mod:info(job_input)),
                    ?assertEqual(1, Mod:info(job_error)),
                    ?assertEqual(2, Mod:info(job_run)),
                    glc:reset_counters(Mod, event_output),
                    ?assertEqual(0, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(event_output)),
                    ?assertEqual(3, Mod:info(job_input)),
                    ?assertEqual(1, Mod:info(job_error)),
                    ?assertEqual(2, Mod:info(job_run)),
                    glc:reset_counters(Mod, job_input),
                    ?assertEqual(0, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(event_output)),
                    ?assertEqual(0, Mod:info(job_input)),
                    ?assertEqual(1, Mod:info(job_error)),
                    ?assertEqual(2, Mod:info(job_run)),
                    glc:reset_counters(Mod, job_error),
                    ?assertEqual(0, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(event_output)),
                    ?assertEqual(0, Mod:info(job_input)),
                    ?assertEqual(0, Mod:info(job_error)),
                    ?assertEqual(2, Mod:info(job_run)),
                    glc:reset_counters(Mod, job_run),
                    ?assertEqual(0, Mod:info(event_input)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(event_output)),
                    ?assertEqual(0, Mod:info(job_input)),
                    ?assertEqual(0, Mod:info(job_error)),
                    ?assertEqual(0, Mod:info(job_run))
                end
            },
            {"variable storage test",
                fun() ->
                    {compiled, Mod} = setup_query(testmod21,
                        glc:eq(a, 2), [{stream, time}]),
                    glc:handle(Mod, gre:make([{'a', 2}], [list])),
                    glc:handle(Mod, gre:make([{'b', 1}], [list])),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    glc:handle(Mod, gre:make([{'b', 2}], [list])),
                    ?assertEqual(3, Mod:info(event_input)),
                    ?assertEqual(2, Mod:info(event_filter)),
                    ?assertEqual({ok, time}, glc:get(Mod, stream)),
                    ?assertEqual({error, undefined}, glc:get(Mod, beam))
                end
            },
            {"with multi function any test",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],

                    G1 = glc:with(glc:eq(a, 1), fun(_Event, EStore) -> 
                       Self ! {a, EStore} end),
                    G2 = glc:with(glc:eq(b, 2), fun(_Event, EStore) -> 
                       Self ! {b, EStore} end),

                    {compiled, Mod} = setup_query(testmod22, any([G1, G2]),
                         Store),
                    glc:handle(Mod, gre:make([{a,1}], [list])),
                    ?assertEqual(1, Mod:info(event_output)),
                    ?assertEqual(a, receive {Msg, _Store} -> Msg after 0 -> notcalled end),
                    ?assertEqual(b, receive {Msg, _Store} -> Msg after 0 -> notcalled end)
                end
            },
            {"with multi function all test",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],

                    G1 = glc:with(glc:eq(a, 1), fun(_Event, EStore) -> 
                       Self ! {a, EStore} end),
                    G2 = glc:with(glc:eq(b, 2), fun(_Event, EStore) -> 
                       Self ! {b, EStore} end),
                    G3 = glc:with(glc:eq(c, 3), fun(_Event, EStore) -> 
                       Self ! {c, EStore} end),

                    {compiled, Mod} = setup_query(testmod23, all([G1, G2, G3]),
                         Store),
                    glc:handle(Mod, gre:make([{a,1}], [list])),
                    ?assertEqual(0, Mod:info(event_output)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    glc:handle(Mod, gre:make([{a,1}, {b, 2}], [list])),
                    ?assertEqual(0, Mod:info(event_output)),
                    ?assertEqual(2, Mod:info(event_filter)),
                    glc:handle(Mod, gre:make([{a,1}, {b, 2}, {c, 3}], [list])),
                    ?assertEqual(1, Mod:info(event_output)),
                    ?assertEqual(a, receive {Msg, _Store} -> Msg after 0 -> notcalled end),
                    ?assertEqual(b, receive {Msg, _Store} -> Msg after 0 -> notcalled end),
                    ?assertEqual(c, receive {Msg, _Store} -> Msg after 0 -> notcalled end)
                end
            },
            {"with multi function match test",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],

                    G1 = glc:with(glc:gt(r, 0.1), fun(_Event, EStore) -> 
                       Self ! {a, EStore} end),
                    G2 = glc:with(glc:all([glc:eq(a, 1), glc:gt(r, 0.5)]), fun(_Event, EStore) -> 
                       Self ! {b, EStore} end),
                    G3 = glc:with(glc:all([glc:eq(a, 1), glc:eq(b, 2), glc:gt(r, 0.6)]), fun(_Event, EStore) -> 
                       Self ! {c, EStore} end),

                    {compiled, Mod} = setup_query(testmod24, [G1, G2, G3],
                         Store),
                    glc:handle(Mod, gre:make([{a,1}, {r, 0.7}, {b, 3}], [list])),
                    ?assertEqual(2, Mod:info(event_output)),
                    ?assertEqual(1, Mod:info(event_input)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(b, receive {Msg, _Store} -> Msg after 0 -> notcalled end),
                    ?assertEqual(a, receive {Msg, _Store} -> Msg after 0 -> notcalled end),
                    %
                    glc:handle(Mod, gre:make([{a,1}, {r, 0.6}], [list])),
                    ?assertEqual(4, Mod:info(event_output)),
                    ?assertEqual(2, Mod:info(event_input)),
                    ?assertEqual(2, Mod:info(event_filter)),
                    ?assertEqual(b, receive {Msg, _Store} -> Msg after 0 -> notcalled end),
                    ?assertEqual(a, receive {Msg, _Store} -> Msg after 0 -> notcalled end),
                    %
                    glc:handle(Mod, gre:make([{a,2}, {r, 0.7}, {b, 3}], [list])),
                    ?assertEqual(5, Mod:info(event_output)),
                    ?assertEqual(3, Mod:info(event_input)),
                    ?assertEqual(4, Mod:info(event_filter)),
                    ?assertEqual(a, receive {Msg, _Store} -> Msg after 0 -> notcalled end),

                    glc:handle(Mod, gre:make([{a,1}, {r, 0.7}, {b, 2}], [list])),
                    ?assertEqual(8, Mod:info(event_output)),
                    ?assertEqual(4, Mod:info(event_input)),
                    ?assertEqual(4, Mod:info(event_filter)),
                    ?assertEqual(c, receive {Msg, _Store} -> Msg after 0 -> notcalled end),
                    ?assertEqual(b, receive {Msg, _Store} -> Msg after 0 -> notcalled end),
                    ?assertEqual(a, receive {Msg, _Store} -> Msg after 0 -> notcalled end)
                end
            },
            {"insert queue job test (linear, no-generate-key)",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],

                    {compiled, Mod} = setup_query(testmod25a,
                        glc:with(glc:gt(a, 14), fun(Event0, _) ->
                            case gre:fetch(a, Event0) of
                                16 -> Self ! {gre:fetch(a, Event0), 
                                              gre:fetch(a, Event0)+1};
                                _ -> ok
                            end
                        end), [{jobs_linearized, true}|Store]), 

                    glc:insert_queue(Mod, <<"5">>, fun(Event2a, _) ->
                        Self ! {q, gre:fetch(a, Event2a)} end, [{a, 14}]),

                    glc:insert_queue(Mod, <<"6">>, fun(Event2b, _) ->
                        Self ! {q, gre:fetch(a, Event2b)} end, [{a, 15}]),

                    glc:insert_queue(Mod, <<"7">>, fun(Event2c, _) ->
                        Self ! {q, gre:fetch(a, Event2c)}, error end, [{a, 16}]),

                    ?assertEqual(14, receive {q, Q} -> Q after 100 -> notcalled end), 
                    ?assertEqual(15, receive {q, Q} -> Q after 100 -> notcalled end), 
                    ?assertEqual(16, receive {q, Q} -> Q after 100 -> notcalled end), 

                    ?assertEqual(notcalled, receive {16, 17} -> ok after 100 -> notcalled end),

                    ?assertEqual(3, Mod:info(queue_input)),
                    ?assertEqual(1, Mod:info(event_output)),

                    glc:insert_queue(Mod, <<"8">>, fun(Event2d, _) ->
                        Self ! {q, gre:fetch(a, Event2d)} end, [{a, 16}]),

                    ?assertEqual(16, receive {q, Q} -> Q after 900 -> notcalled end), 
                    ?assertEqual(ok, receive {16, 17} -> ok after 100 -> notcalled end),


                    glc:insert_queue(Mod, <<"9">>, fun(Event2, _) ->
                        Self ! {q, gre:fetch(a, Event2)} end, [{a, 17}]),
                    ?assertEqual(17, receive {q, Q} -> Q after 100 -> notcalled end), 


                    Worker = workers_name(Mod),

                    timer:sleep(1), % chase your shadow
                    [] = gr_worker:list(Worker),

                    ?assertEqual(5, Mod:info(queue_input)),
                    ?assertEqual(3, Mod:info(event_output)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(4, Mod:info(job_run)),
                    ?assertEqual(1, Mod:info(job_error))

                end
            },
            {"insert queue job test (linear, generate-key)",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],

                    {compiled, Mod} = setup_query(testmod25b,
                        glc:with(glc:gt(a, 14), fun(Event0, _) ->
                            case gre:fetch(a, Event0) of
                                16 -> Self ! {gre:fetch(a, Event0), 
                                              gre:fetch(a, Event0)+1};
                                _ -> ok
                            end
                        end), [{jobs_linearized, true}|Store]), 

                    glc:insert_queue(Mod, undefined, fun(Event2a, _) ->
                        Self ! {q, gre:fetch(a, Event2a)} end, [{a, 14}]),

                    glc:insert_queue(Mod, undefined, fun(Event2b, _) ->
                        Self ! {q, gre:fetch(a, Event2b)} end, [{a, 15}]),

                    glc:insert_queue(Mod, undefined, fun(Event2c, _) ->
                        Self ! {q, gre:fetch(a, Event2c)}, error end, [{a, 16}]),

                    ?assertEqual(14, receive {q, Q} -> Q after 100 -> notcalled end), 
                    ?assertEqual(15, receive {q, Q} -> Q after 100 -> notcalled end), 
                    ?assertEqual(16, receive {q, Q} -> Q after 100 -> notcalled end), 

                    ?assertEqual(notcalled, receive {16, 17} -> ok after 100 -> notcalled end),

                    ?assertEqual(3, Mod:info(queue_input)),
                    ?assertEqual(1, Mod:info(event_output)),

                    glc:insert_queue(Mod, undefined, fun(Event2d, _) ->
                        Self ! {q, gre:fetch(a, Event2d)} end, [{a, 16}]),

                    ?assertEqual(16, receive {q, Q} -> Q after 900 -> notcalled end), 
                    ?assertEqual(ok, receive {16, 17} -> ok after 100 -> notcalled end),


                    glc:insert_queue(Mod, undefined, fun(Event2, _) ->
                        Self ! {q, gre:fetch(a, Event2)} end, [{a, 17}]),
                    ?assertEqual(17, receive {q, Q} -> Q after 100 -> notcalled end), 

                    Worker = workers_name(Mod),

                    timer:sleep(1), 
                    [] = gr_worker:list(Worker),

                    ?assertEqual(5, Mod:info(queue_input)),
                    ?assertEqual(3, Mod:info(event_output)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(4, Mod:info(job_run)),
                    ?assertEqual(1, Mod:info(job_error))

                end
            },
            {"insert queue job test (linear, generate-key, no-stats)",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],

                    {compiled, Mod} = setup_query(testmod25c,
                        glc:with(glc:gt(a, 14), fun(Event0, _) ->
                            case gre:fetch(a, Event0) of
                                16 -> Self ! {gre:fetch(a, Event0), 
                                              gre:fetch(a, Event0)+1};
                                _ -> ok
                            end
                        end), [{jobs_linearized, true},
                               {stats_enabled, false}|Store]), 

                    glc:insert_queue(Mod, undefined, fun(Event2a, _) ->
                        Self ! {q, gre:fetch(a, Event2a)} end, [{a, 14}]),

                    glc:insert_queue(Mod, undefined, fun(Event2b, _) ->
                        Self ! {q, gre:fetch(a, Event2b)} end, [{a, 15}]),

                    glc:insert_queue(Mod, undefined, fun(Event2c, _) ->
                        Self ! {q, gre:fetch(a, Event2c)}, error end, [{a, 16}]),

                    ?assertEqual(14, receive {q, Q} -> Q after 100 -> notcalled end), 
                    ?assertEqual(15, receive {q, Q} -> Q after 100 -> notcalled end), 
                    ?assertEqual(16, receive {q, Q} -> Q after 100 -> notcalled end), 

                    ?assertEqual(notcalled, receive {16, 17} -> ok after 100 -> notcalled end),

                    ?assertEqual(0, Mod:info(queue_input)),
                    ?assertEqual(0, Mod:info(event_output)),

                    glc:insert_queue(Mod, undefined, fun(Event2d, _) ->
                        Self ! {q, gre:fetch(a, Event2d)} end, [{a, 16}]),

                    ?assertEqual(16, receive {q, Q} -> Q after 900 -> notcalled end), 
                    ?assertEqual(ok, receive {16, 17} -> ok after 100 -> notcalled end),


                    glc:insert_queue(Mod, undefined, fun(Event2, _) ->
                        Self ! {q, gre:fetch(a, Event2)} end, [{a, 17}]),
                    ?assertEqual(17, receive {q, Q} -> Q after 100 -> notcalled end), 

                    Worker = workers_name(Mod),

                    timer:sleep(1), % chase your shadow
                    [] = gr_worker:list(Worker),

                    ?assertEqual(0, Mod:info(queue_input)),
                    ?assertEqual(0, Mod:info(event_output)),
                    ?assertEqual(0, Mod:info(event_filter)),
                    ?assertEqual(0, Mod:info(job_run)),
                    ?assertEqual(0, Mod:info(job_error))

                end
            },
            {"insert queue job test (non-linear, generate-key)",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],

                    {compiled, Mod} = setup_query(testmod25d,
                        glc:with(glc:gt(a, 14), fun(Event0, _) ->
                            case gre:fetch(a, Event0) of
                                16 -> Self ! {gre:fetch(a, Event0), 
                                              gre:fetch(a, Event0)+1};
                                _ -> ok
                            end
                        end), [{jobs_linearized, false}|Store]), 

                    glc:insert_queue(Mod, undefined, fun(Event2a, _) ->
                        Self ! {q, gre:fetch(a, Event2a)} end, [{a, 14}]),

                    glc:insert_queue(Mod, undefined, fun(Event2b, _) ->
                        Self ! {q, gre:fetch(a, Event2b)} end, [{a, 15}]),

                    glc:insert_queue(Mod, undefined, fun(Event2c, _) ->
                        Self ! {q, gre:fetch(a, Event2c)}, error end, [{a, 16}]),

                    ?assertEqual([14,15,16], lists:sort([receive {q, Q} -> Q 
                          after 100 -> notcalled end || _ <- lists:seq(1, 3)])), 

                    ?assertEqual(notcalled, receive {16, 17} -> ok after 100 -> notcalled end),

                    ?assertEqual(3, Mod:info(queue_input)),
                    ?assertEqual(1, Mod:info(event_output)),

                    glc:insert_queue(Mod, undefined, fun(Event2d, _) ->
                        Self ! {q, gre:fetch(a, Event2d)} end, [{a, 16}]),

                    ?assertEqual(16, receive {q, Q} -> Q after 900 -> notcalled end), 
                    ?assertEqual(ok, receive {16, 17} -> ok after 100 -> notcalled end),


                    glc:insert_queue(Mod, undefined, fun(Event2, _) ->
                        Self ! {q, gre:fetch(a, Event2)} end, [{a, 17}]),
                    ?assertEqual(17, receive {q, Q} -> Q after 100 -> notcalled end), 

                    Worker = workers_name(Mod),

                    timer:sleep(1), 
                    [] = gr_worker:list(Worker),

                    ?assertEqual(5, Mod:info(queue_input)),
                    ?assertEqual(3, Mod:info(event_output)),
                    ?assertEqual(1, Mod:info(event_filter)),
                    ?assertEqual(4, Mod:info(job_run)),
                    ?assertEqual(1, Mod:info(job_error))

                end
            },
            {"insert queue job test (non-linear, generate-key)",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],
                    JobRuns = 50,
                    JobSeq = lists:seq(1, JobRuns),

                    {ok, Mod} = glc:compile(testmod26, glc:with(glc:gt(a, 10), 
                                            fun(E, _) ->
                                                 Self ! {a, gre:fetch(a,E)}
                                            end), [{jobs_linearized, false}|Store]),

                    Pids = [ spawn(fun() -> 
                         receive go ->  
                             [ glc:insert_queue(Mod, undefined, fun(E, _) -> 
                                        Self ! {q, gre:fetch(a, E)}, ok
                               end, [{a,X}])  || X <- JobSeq ]
                         end 
                        end) || _Y <- [JobRuns] ],
                    [ P ! go || P <- Pids ],

                    wait_for_runs(counts_name(Mod), JobRuns),
                    JobSeq = lists:sort([receive {q, Q} -> Q 
                                         after 1000 -> notcalled 
                                         end || _ <- JobSeq ]),
                    EvtSeq = lists:seq(11, 50),
                    EvtSeq = lists:sort([receive {a, Q} -> Q 
                                         after 1000 -> notcalled 
                                         end || _ <- EvtSeq ]),
                    ?assertEqual(50, Mod:info(queue_input)),
                    ?assertEqual(40, Mod:info(event_output)),
                    ?assertEqual(10, Mod:info(event_filter)),
                    ?assertEqual(50, Mod:info(job_run)),
                    ?assertEqual(0, Mod:info(job_error))


                end
            },
            {"insert queue job test (linear, generate-key)",
                fun() ->
                    Self = self(),
                    Store = [{stored, value}],
                    JobRuns = 50,
                    JobSeq = lists:seq(1, JobRuns),

                    {ok, Mod} = glc:compile(testmod27, glc:with(glc:gt(a, 10), 
                                            fun(E, _) ->
                                                 Self ! {a, gre:fetch(a,E)}
                                            end), [{jobs_linearized, true}|Store]),
                    random:seed(now()),

                    Pids = [ spawn(fun() -> 
                         receive go ->  
                             [ glc:insert_queue(Mod, undefined, fun(E, _) -> 
                                        Self ! {q, gre:fetch(a, E)}, 
                                        timer:sleep(random:uniform(300)),
                                        ok
                               end, [{a,X}])  || X <- JobSeq ]
                         end 
                        end) || _Y <- [JobRuns] ],
                    [ P ! go || P <- Pids ],

                    wait_for_runs(counts_name(Mod), JobRuns),
                    JobSeq = [receive {q, Q} -> Q 
                                         after 1000 -> notcalled 
                                         end || _ <- JobSeq ],
                    EvtSeq = lists:seq(11, 50),
                    EvtSeq = lists:sort([receive {a, Q} -> Q 
                               after 1000 -> notcalled 
                               end || _ <- EvtSeq ]),
                    ?assertEqual(50, Mod:info(queue_input)),
                    ?assertEqual(40, Mod:info(event_output)),
                    ?assertEqual(10, Mod:info(event_filter)),
                    ?assertEqual(50, Mod:info(job_run)),
                    ?assertEqual(0, Mod:info(job_error))


                end
            }
        ]
    }.

union_error_test() ->
    ?assertError(badarg, glc:union([glc:eq(a, 1)])),
    done.

wait_for_runs(Counts, Run) ->
    Runs = gr_counter:lookup_element(Counts, job_run),
    case Runs < Run of
         true -> timer:sleep(1), wait_for_runs(Counts, Run);
         false -> ok
    end.

-endif.
