%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. 4月 2021 下午6:03
%%%-------------------------------------------------------------------
-module(pulsar_consumers_sup).
-author("root").

-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([ensure_present/3, ensure_absence/2]).


start_link() ->
  supervisor:start_link({local, pulsar_consumers_sup},
    pulsar_consumers_sup,
    []).

init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10,
    period => 5},
  Children = [],
  {ok, {SupFlags, Children}}.

ensure_present(ClientId, Topic, ConsumerOpts) ->
  ChildSpec = child_spec(ClientId, Topic, ConsumerOpts),
  case supervisor:start_child(pulsar_consumers_sup,
    ChildSpec)
  of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, {{already_started, Pid}, _}} -> {ok, Pid};
    {error, already_present} -> {error, not_running}
  end.

ensure_absence(ClientId, Name) ->
  Id = {ClientId, Name},
  case supervisor:terminate_child(pulsar_consumers_sup, Id) of
    ok -> ok = supervisor:delete_child(pulsar_consumers_sup, Id);
    {error, not_found} -> ok
  end.

child_spec(ClientId, Topic, ConsumerOpts) ->
  #{id => {ClientId, get_name(ConsumerOpts)},
    start =>
    {pulsar_consumers,
      start_link,
      [ClientId, Topic, ConsumerOpts]},
    restart => transient, type => worker,
    modules => [pulsar_consumers]}.

get_name(ConsumerOpts) ->
  maps:get(name, ConsumerOpts, pulsar_consumers).
