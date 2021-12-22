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


start_link() ->
  supervisor:start_link({local, pulsar_sup}, pulsar_sup, []).

init([]) ->
  SupFlags = #{strategy => one_for_all, intensity => 10, period => 5},
  Children = [client_sup(), producers_sup(), consumers_sup()],
  {ok, {SupFlags, Children}}.

client_sup() ->
  #{id => pulsar_client_sup,
    start => {pulsar_client_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor, modules => [pulsar_client_sup]}.

producers_sup() ->
  #{id => pulsar_producers_sup,
    start => {pulsar_producers_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor, modules => [pulsar_producers_sup]}.

consumers_sup() ->
  #{id => pulsar_consumers_sup,
    start => {pulsar_consumers_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor, modules => [pulsar_consumers_sup]}.
