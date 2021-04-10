%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. 4月 2021 下午6:03
%%%-------------------------------------------------------------------
-module(pulsar_app).
-author("root").
-behaviour(application).

-export([start/2, stop/1]).
%% 启动应用
start(_, _) ->
  pulsar_sup:start_link().

%% 停止应用
stop(_) -> ok.
