%%%-------------------------------------------------------------------
%%% @author root
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. 4ζ 2021 δΈε6:03
%%%-------------------------------------------------------------------
-module(pulsar_app).
-author("root").

-behaviour(application).
-export([start/2, stop/1]).

start(_, _) -> pulsar_sup:start_link().

stop(_) -> ok.
