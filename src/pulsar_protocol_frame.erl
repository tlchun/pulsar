%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. 4ζ 2021 δΈε6:04
%%%-------------------------------------------------------------------
-module(pulsar_protocol_frame).
-author("root").

-export([connect/1,
  topic_metadata/1,
  lookup_topic/1,
  create_producer/1,
  create_subscribe/1,
  set_flow/1,
  ack/1,
  ping/0,
  pong/0,
  serialized_simple_command/1,
  serialized_payload_command/3,
  parse/1,
  send/3]).


connect(CommandConnect) ->
  serialized_simple_command(#{type => 'CONNECT', connect => CommandConnect}).

topic_metadata(PartitionMetadata) ->
  serialized_simple_command(#{type => 'PARTITIONED_METADATA', partitionMetadata => PartitionMetadata}).

lookup_topic(LookupTopic) ->
  serialized_simple_command(#{type => 'LOOKUP', lookupTopic => LookupTopic}).

create_producer(Producer) ->
  serialized_simple_command(#{type => 'PRODUCER', producer => Producer}).

create_subscribe(SubInfo) ->
  serialized_simple_command(#{type => 'SUBSCRIBE', subscribe => SubInfo}).

set_flow(FlowInfo) ->
  serialized_simple_command(#{type => 'FLOW', flow => FlowInfo}).

ack(Ack) ->
  serialized_simple_command(#{type => 'ACK', ack => Ack}).

send(Send, Metadata, BatchPayload) ->
  serialized_payload_command(#{type => 'SEND', send => Send}, i2b(pulsar_api:encode_msg(Metadata, 'MessageMetadata')), BatchPayload).

ping() ->
  serialized_simple_command(#{type => 'PING', ping => #{}}).

pong() ->
  serialized_simple_command(#{type => 'PONG', pong => #{}}).

parse(<<TotalSize:32, CmdBin:TotalSize/binary, Rest/binary>>) ->
  <<CommandSize:32, Command:CommandSize/binary, CmdRest/binary>> = CmdBin,
  BaseCommand = pulsar_api:decode_msg(<<CommandSize:32, Command/binary>>, 'BaseCommand'),
  Resp = case maps:get(type, BaseCommand, unknown) of
           'MESSAGE' ->
             <<MetadataSize:32, Metadata:MetadataSize/binary, Payload0/binary>> = CmdRest,
             MetadataCmd = pulsar_api:decode_msg(<<MetadataSize:32, Metadata/binary>>, 'MessageMetadata'),
             Payloads = parse_batch_message(Payload0, maps:get(num_messages_in_batch, MetadataCmd, 1)),
             {message, maps:get(message, BaseCommand), Payloads};
           'CONNECTED' ->
             {connected, maps:get(connected, BaseCommand)};
           'PARTITIONED_METADATA_RESPONSE' ->
             {partitionMetadataResponse, maps:get(partitionMetadataResponse, BaseCommand)};
           'LOOKUP_RESPONSE' ->
             {lookupTopicResponse, maps:get(lookupTopicResponse, BaseCommand)};
           'PRODUCER_SUCCESS' ->
             {producer_success, maps:get(producer_success, BaseCommand)};
           'SEND_RECEIPT' ->
             {send_receipt, maps:get(send_receipt, BaseCommand)};
           'PING' ->
             {ping, maps:get(ping, BaseCommand)};
           'PONG' ->
             {pong, maps:get(pong, BaseCommand)};
           'CLOSE_PRODUCER' ->
             {close_producer, maps:get(close_producer, BaseCommand)};
           'CLOSE_CONSUMER' ->
             {close_consumer, maps:get(close_consumer, BaseCommand)};
           'SUCCESS' ->
             {subscribe_success, maps:get(success, BaseCommand)};
           _Type ->
             error_logger:error_msg("parse unknown type:~p~n", [BaseCommand]),
             unknown
         end,
  {Resp, Rest};
parse(Bin) -> {undefined, Bin}.

serialized_simple_command(BaseCommand) ->
  BaseCommandBin = i2b(pulsar_api:encode_msg(BaseCommand, 'BaseCommand')),
  Size = size(BaseCommandBin),
  TotalSize = Size + 4,
  <<TotalSize:32, Size:32, BaseCommandBin/binary>>.

serialized_payload_command(BaseCommand, Metadata, BatchPayload) ->
  BaseCommandBin = i2b(pulsar_api:encode_msg(BaseCommand, 'BaseCommand')),
  BaseCommandSize = size(BaseCommandBin),
  MetadataSize = size(Metadata),
  Payload = <<MetadataSize:32, Metadata/binary, BatchPayload/binary>>,
  Checksum = crc32cer:nif(Payload),
  TotalSize = BaseCommandSize + size(Payload) + 10,
  <<TotalSize:32, BaseCommandSize:32, BaseCommandBin/binary, 3585:16, Checksum:32, Payload/binary>>.

i2b(I) when is_list(I) -> iolist_to_binary(I);
i2b(I) -> I.

parse_batch_message(Payloads, Size) ->
  parse_batch_message(Payloads, Size, []).

parse_batch_message(_Payloads, 0, Acc) ->
  lists:reverse(Acc);
parse_batch_message(Payloads, Size, Acc) ->
  <<SMetadataSize:32, SMetadata:SMetadataSize/binary, Rest/binary>> = Payloads,
  SingleMessageMetadata = pulsar_api:decode_msg(<<SMetadataSize:32, SMetadata/binary>>, 'SingleMessageMetadata'),
  PayloadSize = maps:get(payload_size, SingleMessageMetadata),
  <<Payload:PayloadSize/binary, Rest1/binary>> = Rest,
  parse_batch_message(Rest1, Size - 1, [Payload | Acc]).
