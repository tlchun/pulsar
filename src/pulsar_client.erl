%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. 4ζ 2021 δΈε6:03
%%%-------------------------------------------------------------------
-module(pulsar_client).
-author("root").


-behaviour(gen_server).
-export([start_link/3]).

-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-export([get_topic_metadata/2, lookup_topic/2]).
-export([get_status/1]).

-record(state, {sock, servers, opts, producers = #{}, request_id = 0, requests = #{}, from, last_bin = <<>>}).


start_link(ClientId, Servers, Opts) ->
  gen_server:start_link({local, ClientId}, pulsar_client, [Servers, Opts], []).

get_topic_metadata(Pid, Topic) ->
  Call = self(),
  gen_server:call(Pid, {get_topic_metadata, Topic, Call}).

lookup_topic(Pid, PartitionTopic) ->
  gen_server:call(Pid, {lookup_topic, PartitionTopic}, 30000).

get_status(Pid) ->
  gen_server:call(Pid, get_status, 5000).

init([Servers, Opts]) ->
  State = #state{servers = Servers, opts = Opts},
  case get_sock(Servers, undefined) of
    error -> {error, fail_to_connect_pulser_server};
    Sock -> {ok, State#state{sock = Sock}}
  end.

handle_call({get_topic_metadata, Topic, Call}, From,
    State = #state{sock = Sock, request_id = RequestId, requests = Reqs, producers = Producers, servers = Servers}) ->
  case get_sock(Servers, Sock) of
    error ->
      log_error("Servers: ~p down", [Servers]),
      {noreply, State};
    Sock1 ->
      Metadata = topic_metadata(Sock1, Topic, RequestId),
      {noreply, next_request_id(State#state{requests = maps:put(RequestId, {From, Metadata}, Reqs), producers = maps:put(Topic, Call, Producers), sock = Sock1})}
  end;
handle_call({lookup_topic, PartitionTopic}, From, State = #state{sock = Sock, request_id = RequestId, requests = Reqs, servers = Servers}) ->
  case get_sock(Servers, Sock) of
    error ->
      log_error("Servers: ~p down", [Servers]),
      {noreply, State};
    Sock1 ->
      LookupTopic = lookup_topic(Sock1, PartitionTopic, RequestId),
      {noreply,
        next_request_id(State#state{requests = maps:put(RequestId, {From, LookupTopic}, Reqs), sock = Sock1})}
  end;
handle_call(get_status, From,
    State = #state{sock = undefined, servers = Servers}) ->
  case get_sock(Servers, undefined) of
    error -> {reply, false, State};
    Sock -> {noreply, State#state{from = From, sock = Sock}}
  end;
handle_call(get_status, _From, State) ->
  {reply, true, State};
handle_call(_Req, _From, State) ->
  {reply, ok, State, hibernate}.

handle_cast(_Req, State) -> {noreply, State, hibernate}.

handle_info({tcp, _, Bin},
    State = #state{last_bin = LastBin}) ->
  parse(pulsar_protocol_frame:parse(<<LastBin/binary,
    Bin/binary>>),
    State);
handle_info({tcp_closed, Sock},
    State = #state{sock = Sock}) ->
  {noreply, State#state{sock = undefined}, hibernate};
handle_info(ping, State = #state{sock = Sock}) ->
  ping(Sock),
  {noreply, State, hibernate};
handle_info(_Info, State) ->
  log_error("Pulsar_client Receive unknown message:~p~n",
    [_Info]),
  {noreply, State, hibernate}.

terminate(_Reason, #state{}) -> ok.

code_change(_, State, _) -> {ok, State}.

parse({undefined, Bin}, State) ->
  {noreply, State#state{last_bin = Bin}};
parse({Cmd, <<>>}, State) ->
  handle_response(Cmd, State#state{last_bin = <<>>});
parse({Cmd, LastBin}, State) ->
  State2 = case handle_response(Cmd, State) of
             {_, State1} -> State1;
             {_, State1, _} -> State1
           end,
  parse(pulsar_protocol_frame:parse(LastBin), State2).

handle_response({connected, _ConnectedData},
    State = #state{from = undefined}) ->
  start_keepalive(),
  {noreply, State, hibernate};
handle_response({connected, _ConnectedData},
    State = #state{from = From}) ->
  start_keepalive(),
  gen_server:reply(From, true),
  {noreply, State#state{from = undefined}, hibernate};
handle_response({partitionMetadataResponse,
  #{partitions := Partitions, request_id := RequestId}},
    State = #state{requests = Reqs}) ->
  case maps:get(RequestId, Reqs, undefined) of
    {From, #{topic := Topic}} ->
      gen_server:reply(From, {Topic, Partitions}),
      {noreply,
        State#state{requests = maps:remove(RequestId, Reqs)},
        hibernate};
    undefined -> {noreply, State, hibernate}
  end;
handle_response({lookupTopicResponse,
  #{brokerServiceUrl := BrokerServiceUrl,
    request_id := RequestId}},
    State = #state{requests = Reqs}) ->
  case maps:get(RequestId, Reqs, undefined) of
    {From, #{}} ->
      gen_server:reply(From, BrokerServiceUrl),
      {noreply,
        State#state{requests = maps:remove(RequestId, Reqs)},
        hibernate};
    undefined -> {noreply, State, hibernate}
  end;
handle_response({ping, #{}},
    State = #state{sock = Sock}) ->
  pong(Sock),
  {noreply, State, hibernate};
handle_response({pong, #{}}, State) ->
  start_keepalive(),
  {noreply, State, hibernate};
handle_response(_Info, State) ->
  log_error("Client handle_response unknown message:~p~n",
    [_Info]),
  {noreply, State, hibernate}.

tune_buffer(Sock) ->
  {ok, [{recbuf, RecBuf}, {sndbuf, SndBuf}]} =
    inet:getopts(Sock, [recbuf, sndbuf]),
  inet:setopts(Sock, [{buffer, max(RecBuf, SndBuf)}]).

get_sock(Servers, undefined) -> try_connect(Servers);
get_sock(_Servers, Sock) -> Sock.

try_connect([]) -> error;
try_connect([{Host, Port} | Servers]) ->
  case gen_tcp:connect(Host,
    Port,
    [binary, {packet, raw}, {nodelay, true}, {active, true}, {reuseaddr, true}, {send_timeout, 60000}],
    60000)
  of
    {ok, Sock} ->
      tune_buffer(Sock),
      gen_tcp:controlling_process(Sock, self()),
      connect(Sock),
      Sock;
    _Error -> try_connect(Servers)
  end.

connect(Sock) ->
  Conn = #{client_version =>
  "Pulsar-Client-Erlang-v0.0.1",
    protocol_version => 6},
  gen_tcp:send(Sock, pulsar_protocol_frame:connect(Conn)).

topic_metadata(Sock, Topic, RequestId) ->
  Metadata = #{topic => Topic, request_id => RequestId},
  gen_tcp:send(Sock,
    pulsar_protocol_frame:topic_metadata(Metadata)),
  Metadata.

lookup_topic(Sock, Topic, RequestId) ->
  LookupTopic = #{topic => Topic,
    request_id => RequestId},
  gen_tcp:send(Sock,
    pulsar_protocol_frame:lookup_topic(LookupTopic)),
  LookupTopic.

next_request_id(State = #state{request_id = 65535}) ->
  State#state{request_id = 1};
next_request_id(State = #state{request_id =
RequestId}) ->
  State#state{request_id = RequestId + 1}.

log_error(Fmt, Args) ->
  error_logger:error_msg(Fmt, Args).

start_keepalive() ->
  erlang:send_after(30 * 1000, self(), ping).

ping(Sock) ->
  gen_tcp:send(Sock, pulsar_protocol_frame:ping()).

pong(Sock) ->
  gen_tcp:send(Sock, pulsar_protocol_frame:pong()).
