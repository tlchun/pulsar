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


start_link() ->
  supervisor:start_link({local, pulsar_producers_sup}, pulsar_producers_sup, []).

init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  Children = [],
  {ok, {SupFlags, Children}}.

ensure_present(ClientId, Topic, ProducerOpts) ->
  ChildSpec = child_spec(ClientId, Topic, ProducerOpts),
  case supervisor:start_child(pulsar_producers_sup, ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, {{already_started, Pid}, _}} -> {ok, Pid};
    {error, already_present} -> {error, not_running}
  end.

ensure_absence(ClientId, Name) ->
  Id = {ClientId, Name},
  case supervisor:terminate_child(pulsar_producers_sup, Id) of
    ok ->
      ok = supervisor:delete_child(pulsar_producers_sup, Id);
    {error, not_found} -> ok
  end.

child_spec(ClientId, Topic, ProducerOpts) ->
  #{id => {ClientId, get_name(ProducerOpts)},
    start => {pulsar_producers, start_link, [ClientId, Topic, ProducerOpts]},
    restart => transient, type => worker,
    modules => [pulsar_producers]}.

get_name(ProducerOpts) ->
  maps:get(name, ProducerOpts, pulsar_producers).


