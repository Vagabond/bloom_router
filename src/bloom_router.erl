-module(bloom_router).

-export([start/0]).

%-define(XORType, xor8).
-define(XORType, xor16).

%-define(XORHash, fun erlang:phash2/1).
-define(XORHash, fun xxhash:hash64/1).

start() ->
    Path = "/tmp/bloom_router.db",
    Options = [{create_if_missing, true}],
    rocksdb:destroy(Path, Options),
    {ok, DB} = rocksdb:open(Path, Options),
    io:format("generating OUIs~n"),
    NumOUIs    = 10000,
    NumDevices = 8000000,
    NumTrials  = 200,
    OUIsAndDeviceCounts = [ {OUI, rand:uniform(1000000)} || OUI <- lists:seq(1, NumOUIs) ],
    io:format("generated ~p OUIs holding ~p devices~n", [NumOUIs, lists:sum(element(2, lists:unzip(OUIsAndDeviceCounts)))]),
    io:format("generating ~p devices~n", [NumDevices]),
    DevicesWithOUIs = [ {rand:uniform(4294967296), rand:uniform(NumOUIs)} || _ <- lists:seq(1, NumDevices) ],
    io:format("populating ~p OUIs with ~p devices~n", [NumOUIs, NumDevices]),
    pmap(fun({OUI, Count}) ->
                 %{ok, Bloom} = bloom:new_for_fp_rate(Count, 0.000000005),
                 %[ bloom:set(Bloom, <<Device:32/integer-unsigned-big>>) || {Device, OUI0} <- DevicesWithOUIs, OUI0 == OUI ],
                 %rocksdb:put(DB, <<OUI:32/integer-unsigned-big>>, bloom:to_bin(Bloom), [])
                 Xor = ?XORType:new(lists:usort([Device || {Device, OUI0} <- DevicesWithOUIs, OUI0 == OUI ]), ?XORHash),
                 rocksdb:put(DB, <<OUI:32/integer-unsigned-big>>,?XORType:to_bin(Xor), [])
         end, OUIsAndDeviceCounts),
    io:format("compacting DB~n"),
    rocksdb:compact_range(DB, undefined, undefined, []),
    timer:sleep(5000),
    io:format("Attempting ~p random lookups~n", [NumTrials]),
    RandomDevices = [ lists:nth(rand:uniform(NumDevices), DevicesWithOUIs) || _ <- lists:seq(1, NumTrials)],
    {Misses, Times, Errors} = lists:unzip3([ trial(DB, E) || E <- RandomDevices]),
    io:format("Average errors ~p, max ~p, min ~p~n", [lists:sum(Errors)/NumTrials, lists:max(Errors), lists:min(Errors)]),
    io:format("Average lookup ~p, max ~p, min ~p~n", [(lists:sum(Times)/NumTrials) / 1000000, lists:max(Times) / 1000000, lists:min(Times) / 1000000]),
    io:format("Lookup misses ~p~n", [lists:sum(Misses)]),
    ok.

trial(DB, {Device, OUI}) ->
    {ok, Itr} = rocksdb:iterator(DB, []),
    {Time, Res} = timer:tc(fun() -> check(Itr, rocksdb:iterator_move(Itr, first), Device, []) end),
    rocksdb:iterator_close(Itr),
    io:format("Device ~.16b was found in ~p OUIs in ~p seconds, found correct OUI ~p~n", [Device, length(Res), Time / 1000000, lists:member(OUI, Res)]),
    case lists:member(OUI, Res) of
        true ->
            {0, Time, length(Res) - 1};
        false ->
            {1, Time, length(Res)}
    end.

check(_Itr, {error, E}, _Device, Acc) ->
    Acc;
check(Itr, {ok, <<OUI:32/integer-unsigned-big>>, B}, Device, Acc0) ->
    %Acc = case bloom:check(B, <<Device:32/integer-unsigned-big>>) of
    Acc = case ?XORType:contain(?XORType:from_bin(B, ?XORHash), Device) of
        true ->
            [OUI | Acc0];
        false ->
            Acc0
    end,
    check(Itr, rocksdb:iterator_move(Itr, next), Device, Acc).

pmap(F, L) ->
    Width = erlang:system_info(schedulers) + 2,
    pmap(F, L, Width).

pmap(F, L, Width) ->
    Parent = self(),
    Len = length(L),
    Min = floor(Len/Width),
    Rem = Len rem Width,
    Lengths = lists:duplicate(Rem, Min+1)++ lists:duplicate(Width - Rem, Min),
    OL = partition_list(L, Lengths, []),
    St = lists:foldl(
           fun([], N) ->
                   N;
              (IL, N) ->
                   spawn_opt(
                     fun() ->
                             Parent ! {pmap, N, lists:map(F, IL)}
                     end, [{fullsweep_after, 0}]),
                   N+1
           end, 0, OL),
    L2 = [receive
              {pmap, N, R} -> {N,R}
          end || _ <- lists:seq(1, St)],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    lists:flatten(L3).

partition_list([], [], Acc) ->
    lists:reverse(Acc);
partition_list(L, [0 | T], Acc) ->
    partition_list(L, T, Acc);
partition_list(L, [H | T], Acc) ->
    {Take, Rest} = lists:split(H, L),
    partition_list(Rest, T, [Take | Acc]).
