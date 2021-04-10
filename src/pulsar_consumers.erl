%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. 4月 2021 下午6:03
%%%-------------------------------------------------------------------
-module(pulsar_consumers).
-author("root").

-export([start_supervised/3, stop_supervised/1, start_link/3]).

-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, terminate/2]).

-record(state, {topic, client_id, partitions, consumer_opts, consumers = #{}}).

%% 启动监督
start_supervised(ClientId, Topic, ConsumerOpts) ->
  {ok, _Pid} = pulsar_consumers_sup:ensure_present(ClientId, Topic, ConsumerOpts).

%% 停止监督
stop_supervised(#{client := ClientId, name := Name}) ->
  pulsar_consumers_sup:ensure_absence(ClientId, Name).

start_link(ClientId, Topic, ConsumerOpts) ->
  gen_server:start_link({local, get_name(ConsumerOpts)}, pulsar_consumers, [ClientId, Topic, ConsumerOpts], []).

init([ClientId, Topic, ConsumerOpts]) ->
  erlang:process_flag(trap_exit, true),
  {ok, #state{topic = Topic, client_id = ClientId, consumer_opts = ConsumerOpts}, 0}.

handle_call(_Call, _From, State) ->
  {reply, {error, unknown_call}, State}.

handle_cast(_Cast, State) -> {noreply, State}.

handle_info(timeout,
    State = #state{client_id = ClientId, topic = Topic,
      consumers = Consumers,
      consumer_opts = ConsumerOpts}) ->
  case pulsar_client_sup:find_client(ClientId) of
    {ok, Pid} ->
      {_, Partitions} = pulsar_client:get_topic_metadata(Pid,
        Topic),
      PartitionTopics = create_partition_topic(Topic,
        Partitions),
      NewConsumers = lists:foldl(fun (PartitionTopic, Acc) ->
        start_consumer(Pid,
          PartitionTopic,
          ConsumerOpts,
          Acc)
                                 end,
        Consumers,
        PartitionTopics),
      {noreply,
        State#state{partitions = length(PartitionTopics),
          consumers = NewConsumers}};
    {error, Reason} -> {stop, {shutdown, Reason}, State}
  end;
handle_info({'EXIT', Pid, _Error},
    State = #state{consumers = Consumers}) ->
  case maps:get(Pid, Consumers, undefined) of
    undefined ->
      log_error("Not find Pid:~p consumer", [Pid]),
      {noreply, State};
    PartitionTopic ->
      self() ! {restart_consumer, PartitionTopic},
      {noreply,
        State#state{consumers = maps:remove(Pid, Consumers)}}
  end;
handle_info({restart_consumer, PartitionTopic},
    State = #state{client_id = ClientId,
      consumers = Consumers,
      consumer_opts = ConsumerOpts}) ->
  case pulsar_client_sup:find_client(ClientId) of
    {ok, Pid} ->
      NewConsumers = start_consumer(Pid,
        PartitionTopic,
        ConsumerOpts,
        Consumers),
      {noreply, State#state{consumers = NewConsumers}};
    {error, Reason} -> {stop, {shutdown, Reason}, State}
  end;
handle_info(_Info, State) ->
  log_error("Receive unknown message:~p~n", [_Info]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_, _St) -> ok.

create_partition_topic(Topic, 0) -> [Topic];
create_partition_topic(Topic, Partitions) ->
  lists:map(fun (Partition) ->
    lists:concat([Topic, "-partition-", Partition])
            end,
    lists:seq(0, Partitions - 1)).

get_name(ConsumerOpts) ->
  maps:get(name, ConsumerOpts, pulsar_consumers).

log_error(Fmt, Args) ->
  error_logger:error_msg(Fmt, Args).

start_consumer(Pid, PartitionTopic, ConsumerOpts,
    Consumers) ->
  try BrokerServiceUrl = pulsar_client:lookup_topic(Pid,
    PartitionTopic),
  {MaxConsumerMum, ConsumerOpts1} = case
                                      maps:take(max_consumer_num,
                                        ConsumerOpts)
                                    of
                                      error -> {1, ConsumerOpts};
                                      Res -> Res
                                    end,
  lists:foldl(fun (_, Acc) ->
    {ok, Consumer} =
      pulsar_consumer:start_link(PartitionTopic,
        BrokerServiceUrl,
        ConsumerOpts1),
    maps:put(Consumer, PartitionTopic, Acc)
              end,
    Consumers,
    lists:seq(1, MaxConsumerMum))
  catch
    Error:Reason:Stacktrace ->
      log_error("Start consumer: ~p, ~p",
        [Error, {Reason, Stacktrace}]),
      self() ! {restart_consumer, PartitionTopic},
      Consumers
  end.

