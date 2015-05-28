%% Copyright (c) 2015, Pedram Nimreezi <deadzen@deadzen.com>
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

%% @doc Worker supervisor for all goldrush job runs.
%%
-module(gr_worker_job_sup).
-behaviour(supervisor).

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

%% API
-export([start_link/6]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================
%% @hidden
-spec start_link(atom(), atom(), atom(), boolean(), pos_integer(),
                 pos_integer()) -> startlink_ret().
start_link(Name, Module, Reporter, StatsEnabled, Limit, Width) ->
    supervisor:start_link({local, Name}, ?MODULE, [Module, Reporter, 
                                                   StatsEnabled, Limit, Width]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
%% @hidden
-spec init([]) -> {ok, { {simple_one_for_one, 50, 10}, [supervisor:child_spec()]} }.
init([Module, Reporter, StatsEnabled, Limit, Width]) ->
    ok = init_workers(Module, Width),
    %% init stats
    ChildSpec = {gr_worker_job,
                 {gr_worker_job, start_link, [Module, Reporter, StatsEnabled, 
                                              Limit, Width]},
                 temporary, brutal_kill, worker, [gr_worker_job]},
    {ok, { {simple_one_for_one, 50, 10}, [ChildSpec]}}.

-spec init_workers(atom(), pos_integer() | [atom()]) -> ok.
init_workers(Module, Width) when is_integer(Width) ->
    Workers = glc:job_workers(Module, Width),
    init_workers(Module, Workers);
init_workers(Module, [Worker|Workers]) ->
    TableId = ets:new(Worker, [named_table, public]), %% @todo make not public
    true = ets:insert(TableId, [{usage, 0}, {full, 0}]),
    init_workers(Module, Workers);
init_workers(_Module, []) ->
    ok.




