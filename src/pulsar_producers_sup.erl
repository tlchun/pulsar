%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. 4月 2021 下午6:04
%%%-------------------------------------------------------------------
-module(pulsar_producers_sup).
-author("root").

-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([ensure_present/3, ensure_absence/2]).

%% 生产者进程启动
start_link() ->
  supervisor:start_link({local, pulsar_consumers_sup}, pulsar_consumers_sup, []).

%% 监控进程回调
init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  Children = [],
  {ok, {SupFlags, Children}}.

%% 确保存在
ensure_present(ClientId, Topic, ConsumerOpts) ->
  ChildSpec = child_spec(ClientId, Topic, ConsumerOpts),
%%  启动生产者下面的子进程
  case supervisor:start_child(pulsar_consumers_sup, ChildSpec) of
%%    启动成功
    {ok, Pid} -> {ok, Pid};
%%    已经启动
    {error, {already_started, Pid}} -> {ok, Pid};
%%    已经启动
    {error, {{already_started, Pid}, _}} -> {ok, Pid};
%%    已经存在，但是不运行
    {error, already_present} -> {error, not_running}
  end.

%% 确保不存在
ensure_absence(ClientId, Name) ->
  Id = {ClientId, Name},
%%  终止
  case supervisor:terminate_child(pulsar_consumers_sup, Id) of
%%    删除
    ok -> ok = supervisor:delete_child(pulsar_consumers_sup, Id);
%%    删除没发现
    {error, not_found} -> ok
  end.

child_spec(ClientId, Topic, ConsumerOpts) ->
  #{id => {ClientId, get_name(ConsumerOpts)},
    start => {pulsar_consumers, start_link, [ClientId, Topic, ConsumerOpts]}, restart => transient, type => worker,
    modules => [pulsar_consumers]}.

get_name(ConsumerOpts) ->
  maps:get(name, ConsumerOpts, pulsar_consumers).

