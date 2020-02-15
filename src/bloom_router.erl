-module(bloom_router).

-export([start/3]).

%-define(XORType, xor8).
-define(XORType, xor16).

%-define(XORHash, fun erlang:phash2/1).
-define(XORHash, fun xxhash:hash64/1).

start(Type, NumOUIs, NumDevices) when NumDevices > NumOUIs ->
    case Type of
        bloom ->
            XorType = undefined,
            XorHash = undefined,
            ok;
        xor8_phash ->
            XorType = xor8,
            XorHash = fun erlang:phash2/1;
        xor8_xxhash ->
            XorType = xor8,
            XorHash = fun xxhash:hash64/1;
        xor16_phash ->
            XorType = xor16,
            XorHash = fun erlang:phash2/1;
        xor16_xxhash ->
            XorType = xor16,
            XorHash = fun xxhash:hash64/1
    end,
    io:format("~nrunning ~p with ~p OUIs and ~p Devices~n", [Type, NumOUIs, NumDevices]),
    Path = lists:flatten(io_lib:format("/tmp/bloom_router-~s-~b-~b.db", [Type, NumOUIs, NumDevices])),
    Options = [{create_if_missing, true}],
    {ok, DB} = rocksdb:open(Path, Options),
    RandomDevices = case rocksdb:get(DB, <<"randomdevices">>, []) of
        not_found ->
            io:format("generating OUIs~n"),
            NumTrials  = 1000,
            %OUIsAndDeviceCounts = [ {OUI, rand:uniform(1000000)} || OUI <- lists:seq(1, NumOUIs) ],
            OUIs = lists:seq(1, NumOUIs),
            %io:format("generated ~p OUIs holding ~p devices~n", [NumOUIs, lists:sum(element(2, lists:unzip(OUIsAndDeviceCounts)))]),
            io:format("generated ~p OUIs~n", [NumOUIs]),
            io:format("generating ~p devices~n", [NumDevices]),
            DevicesWithOUIs = [ {rand:uniform(4294967296), rand:uniform(NumOUIs)} || _ <- lists:seq(1, NumDevices) ],
            io:format("populating ~p OUIs with ~p devices~n", [NumOUIs, NumDevices]),
            %pmap(fun({OUI, Count}) ->
            pmap(fun(OUI) ->
                         case Type of
                             bloom ->
                                 DevicesForThisOUI = [Device || {Device, OUI0} <- DevicesWithOUIs, OUI0 == OUI ],
                                 %% build a bloom for this exact capacity
                                 {ok, Bloom} = bloom:new_for_fp_rate(length(DevicesForThisOUI), 0.000000005),
                                 [ bloom:set(Bloom, <<Device:32/integer-unsigned-big>>) || Device <- DevicesForThisOUI ],
                                 rocksdb:put(DB, <<OUI:32/integer-unsigned-big>>, bloom:to_bin(Bloom), []);
                             _ ->
                                 Xor = XorType:new(lists:usort([Device || {Device, OUI0} <- DevicesWithOUIs, OUI0 == OUI ]), XorHash),
                                 rocksdb:put(DB, <<OUI:32/integer-unsigned-big>>, XorType:to_bin(Xor), [])
                         end
                 end, OUIs),
            io:format("compacting DB~n"),
            rocksdb:compact_range(DB, undefined, undefined, []),
            timer:sleep(5000),
            io:format("Attempting ~p random lookups~n", [NumTrials]),
            RD = [ lists:nth(rand:uniform(NumDevices), DevicesWithOUIs) || _ <- lists:seq(1, NumTrials)],
            rocksdb:put(DB, <<"randomdevices">>, term_to_binary(RD), []),
            RD;
        {ok, BRD} ->
            RD = binary_to_term(BRD),
            NumTrials = length(RD),
            io:format("Attempting ~p random lookups~n", [NumTrials]),
            RD
    end,

    Parent = self(),
    Fun = fun F({Min, Max, Avg}=Acc) ->
    receive done ->
                Parent ! {memory, Acc}
    after 10 ->
              Current = element(2, hd(erlang:memory())),
              F({erlang:min(Min, Current), erlang:max(Max, Current), (Avg + Current) / 2})
    end
      end,
    erlang:garbage_collect(),
    BaselineMemory = element(2, hd(erlang:memory())),

    MonPid = spawn(fun() -> Fun({infinity, 0, BaselineMemory}) end),
    {Misses, Times, Errors} = lists:unzip3([ trial(DB, E, Type, XorType, XorHash) || E <- RandomDevices]),
    MonPid ! done,
    receive {memory, {Min, Max, Avg}} ->
                io:format("Average memory ~.2fMb, max ~.2fMb, min ~.2fMb~n", [(Avg - BaselineMemory)/(1024*1024),
                                                                              (Max - BaselineMemory)/(1024*1024),
                                                                              (Min - BaselineMemory)/(1024*1024)])
    end,
    [Size] = rocksdb:get_approximate_sizes(DB, [{<<0:32/integer-unsigned-big>>, <<(NumOUIs+1):32/integer-unsigned-big>>}], include_files),
    io:format("Approximate database size ~.2fMb~n", [Size/(1024*1024)]),
    io:format("Average errors ~.3f, max ~p, min ~p~n", [lists:sum(Errors)/NumTrials, lists:max(Errors), lists:min(Errors)]),
    io:format("Average lookup ~.3fs, max ~.3fs, min ~.3fs~n", [(lists:sum(Times)/NumTrials) / 1000000, lists:max(Times) / 1000000, lists:min(Times) / 1000000]),
    io:format("Lookup misses ~p~n~n", [lists:sum(Misses)]),
    rocksdb:close(DB),
    erlang:garbage_collect(),
    ok.

trial(DB, {Device, OUI}, Type, XorType, XorHash) ->
    {ok, Itr} = rocksdb:iterator(DB, []),
    Fun = case Type of
              bloom ->
                  fun(B) ->
                      bloom:check(B, <<Device:32/integer-unsigned-big>>)
                  end;
              _ ->
                  fun(B) ->
                      XorType:contain(XorType:from_bin(B, XorHash), Device)
                  end
          end,
    {Time, Res} = timer:tc(fun() -> check(Itr, rocksdb:iterator_move(Itr, first), Fun, []) end),
    rocksdb:iterator_close(Itr),
    %io:format("Device ~.16b was found in ~p OUIs in ~p seconds, found correct OUI ~p~n", [Device, length(Res), Time / 1000000, lists:member(OUI, Res)]),
    case lists:member(OUI, Res) of
        true ->
            {0, Time, length(Res) - 1};
        false ->
            {1, Time, length(Res)}
    end.

check(_Itr, {error, _E}, _Fun, Acc) ->
    Acc;
check(Itr, {ok, <<"randomdevices">>, _}, Fun, Acc0) ->
    check(Itr, rocksdb:iterator_move(Itr, next), Fun, Acc0);
check(Itr, {ok, <<OUI:32/integer-unsigned-big>>, B}, Fun, Acc0) ->
    Acc = case Fun(B) of
        true ->
            [OUI | Acc0];
        false ->
            Acc0
    end,
    check(Itr, rocksdb:iterator_move(Itr, next), Fun, Acc).

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
