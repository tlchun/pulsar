{application,pulsar,
             [{description,"A Erlang client library for Apache Pulsar"},
              {vsn,"1.0.0"},
              {modules,[
                pulsar,
                pulsar_api,
                pulsar_app,
                pulsar_client,
                pulsar_client_sup,
                pulsar_consumer,
                pulsar_consumers,
                pulsar_consumers_sup,
                pulsar_producer,
                pulsar_producers,
                pulsar_producers_sup,
                pulsar_protocol_frame,
                pulsar_sup]},
              {registered,[pulsar_sup]},
              {applications,[kernel,stdlib,crc32cer,murmerl3]},
              {mod,{pulsar_app,[]}}]}.

%% {ok,{_,[{abstract_code,{_,AC}}]}} = beam_lib:chunks(pulsar_consumers,[abstract_code]).
%% io:fwrite("~s~n", [erl_prettypr:format(erl_syntax:form_list(AC))]).
