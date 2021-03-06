%%%-------------------------------------------------------------------
%% @doc bloom_router public API
%% @end
%%%-------------------------------------------------------------------

-module(bloom_router_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    bloom_router_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
