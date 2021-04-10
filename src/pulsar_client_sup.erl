%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. 4月 2021 下午6:03
%%%-------------------------------------------------------------------
-module(pulsar_client_sup).
-author("root").

-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([ensure_present/3, ensure_absence/1, find_client/1]).

%% 客户端监控进程启动
start_link() ->
  supervisor:start_link({local, pulsar_client_sup}, pulsar_client_sup, []).

%% 监控行为回调函数
init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  Children = [],
  {ok, {SupFlags, Children}}.

%% 确认存在客户端
ensure_present(ClientId, Hosts, Opts) ->
%% 子进程描述
  ChildSpec = child_spec(ClientId, Hosts, Opts),
%%  客户端监控进程下 启动 子进程
  case supervisor:start_child(pulsar_client_sup, ChildSpec) of
%%    启动成功，返回进程ID
    {ok, Pid} -> {ok, Pid};
%%    已经启动
    {error, {already_started, Pid}} -> {ok, Pid};
%%    已经存在，客户端没运行
    {error, already_present} -> {error, client_not_running}
  end.

%% 确保客户端不存在
ensure_absence(ClientId) ->
%%  终止
  case supervisor:terminate_child(pulsar_client_sup, ClientId) of
%%    终止成功，然后删除
    ok -> ok = supervisor:delete_child(pulsar_client_sup, ClientId);
%%   终止没找到，返回成功
    {error, not_found} -> ok
  end.

%% 获取客户端
find_client(ClientId) ->
%%  查找pulsar_client_sup 监控进程下的进程列表
  Children = supervisor:which_children(pulsar_client_sup),
%%  遍历寻找
  case lists:keyfind(ClientId, 1, Children) of
    {ClientId, Client, _, _} when is_pid(Client) -> {ok, Client};
    {ClientId, Restarting, _, _} -> {error, Restarting};
    false -> erlang:error({no_such_client, ClientId})
  end.

child_spec(ClientId, Hosts, Opts) ->
  #{id => ClientId,
    start =>
    {pulsar_client, start_link, [ClientId, Hosts, Opts]},
    restart => transient, type => worker,
    modules => [pulsar_client]}.


