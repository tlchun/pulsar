%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. 4月 2021 下午6:03
%%%-------------------------------------------------------------------
-module(pulsar).
-author("root").


-export([start/0]).

-export([ensure_supervised_client/3, stop_and_delete_supervised_client/1]).
-export([ensure_supervised_producers/3, stop_and_delete_supervised_producers/1]).
-export([ensure_supervised_consumers/3, stop_and_delete_supervised_consumers/1]).

-export([send/2, send_sync/2, send_sync/3]).

start() ->
  application:start(crc32cer),
  application:start(pulsar).

ensure_supervised_client(ClientId, Hosts, Opts) ->
  pulsar_client_sup:ensure_present(ClientId, Hosts, Opts).

stop_and_delete_supervised_client(ClientId) ->
  pulsar_client_sup:ensure_absence(ClientId).

ensure_supervised_producers(ClientId, Topic, Opts) ->
  pulsar_producers:start_supervised(ClientId, Topic, Opts).

stop_and_delete_supervised_producers(Producers) ->
  pulsar_producers:stop_supervised(Producers).

ensure_supervised_consumers(ClientId, Topic, Opts) ->
  pulsar_consumers:start_supervised(ClientId, Topic, Opts).

stop_and_delete_supervised_consumers(Consumers) ->
  pulsar_consumers:stop_supervised(Consumers).

send(Producers, Batch) ->
  {_Partition, ProducerPid} = pulsar_producers:pick_producer(Producers, Batch),
  pulsar_producer:send(ProducerPid, Batch).

send_sync(Producers, Batch, Timeout) ->
  {_Partition, ProducerPid} = pulsar_producers:pick_producer(Producers, Batch),
  pulsar_producer:send_sync(ProducerPid, Batch, Timeout).

send_sync(Producers, Batch) ->
  {_Partition, ProducerPid} = pulsar_producers:pick_producer(Producers, Batch),
  pulsar_producer:send_sync(ProducerPid, Batch).

