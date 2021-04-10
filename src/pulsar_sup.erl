%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. 4月 2021 下午6:04
%%%-------------------------------------------------------------------
-module(pulsar_sup).
-author("root").
-behaviour(supervisor).

-export([start_link/0, init/1]).

%% 启动应用根监控进程
start_link() ->
  supervisor:start_link({local, pulsar_sup}, pulsar_sup, []).

%% 监控回调模块
init([]) ->
%%  监控和重启策略设置
  SupFlags = #{strategy => one_for_all, intensity => 10, period => 5},
%%  子进程列表 客户监控，生产者监控，消费者监控
  Children = [client_sup(), producers_sup(), consumers_sup()],
  {ok, {SupFlags, Children}}.

%% 客户监控进程定义
client_sup() ->
  #{id => pulsar_client_sup,
    start => {pulsar_client_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor, modules => [pulsar_client_sup]}.
%% 生产者监控进程定义
producers_sup() ->
  #{id => pulsar_producers_sup,
    start => {pulsar_producers_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor, modules => [pulsar_producers_sup]}.
%% 消费者监控进程定义
consumers_sup() ->
  #{id => pulsar_consumers_sup,
    start => {pulsar_consumers_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor, modules => [pulsar_consumers_sup]}.
