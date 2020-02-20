-module(range_router).

-export([start/2]).

start(NumOUIs, NumDevices) ->
    io:format("~nrunning with ~p OUIs and ~p Devices~n", [NumOUIs, NumDevices]),
    Path = lists:flatten(io_lib:format("/tmp/range_router-~b-~b.db", [NumOUIs, NumDevices])),
    Options = [{create_if_missing, true}],
    {ok, DB} = rocksdb:open(Path, Options),
    RandomDevices = case rocksdb:get(DB, <<"randomdevices">>, []) of
        not_found ->
            io:format("generating OUIs~n"),
            NumTrials  = 1000,
            OUIs = lists:seq(1, NumOUIs),
            io:format("generated ~p OUIs~n", [NumOUIs]),
            io:format("generating ~p devices~n", [NumDevices]),
            DevicesWithOUIs = [ {rand:uniform(4294967296), rand:uniform(NumOUIs)} || _ <- lists:seq(1, NumDevices) ],
            {_End, DeviceMapping} = lists:foldl(fun(OUI, {Start, Map}) ->
                                DevicesForThisOUI = [Device || {Device, OUI0} <- DevicesWithOUIs, OUI0 == OUI ],
                                Size = allocation_size(length(DevicesForThisOUI)),
                                rocksdb:put(DB, <<Start:32/integer-unsigned-big>>, <<OUI:32/integer-unsigned-big>>, []),
                                {Start+Size, maps:merge(Map, maps:from_list(lists:zip(DevicesForThisOUI, lists:seq(Start, Start + length(DevicesForThisOUI) - 1))))}
                        end, {0, #{}}, OUIs),
            io:format("compacting DB~n"),
            rocksdb:compact_range(DB, undefined, undefined, []),
            timer:sleep(5000),
            io:format("Attempting ~p random lookups~n", [NumTrials]),
            RD = [ lists:nth(rand:uniform(NumDevices), DevicesWithOUIs) || _ <- lists:seq(1, NumTrials)],
            rocksdb:put(DB, <<"randomdevices">>, term_to_binary(RD), []),
            rocksdb:put(DB, <<"mapping">>, term_to_binary(DeviceMapping), []),
            RD;
        {ok, BRD} ->
            RD = binary_to_term(BRD),
            NumTrials = length(RD),
            {ok, BMapping} = rocksdb:get(DB, <<"mapping">>, []),
            DeviceMapping = binary_to_term(BMapping),
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
    {Misses, Times} = lists:unzip([ trial(DB, E, DeviceMapping) || E <- RandomDevices]),
    MonPid ! done,
    receive {memory, {Min, Max, Avg}} ->
                io:format("Average memory ~.2fMb, max ~.2fMb, min ~.2fMb~n", [(Avg - BaselineMemory)/(1024*1024),
                                                                              (Max - BaselineMemory)/(1024*1024),
                                                                              (Min - BaselineMemory)/(1024*1024)])
    end,
    [Size] = rocksdb:get_approximate_sizes(DB, [{<<0:32/integer-unsigned-big>>, <<(NumDevices+1):32/integer-unsigned-big>>}], include_files),
    io:format("Approximate database size ~sMb~n", [sigfigs(Size/(1024*1024), 2)]),
    %io:format("Average errors ~s, max ~p, min ~p~n", [sigfigs(lists:sum(Errors)/NumTrials, 2), lists:max(Errors), lists:min(Errors)]),
    io:format("Average lookup ~ss, max ~ss, min ~ss~n", [sigfigs((lists:sum(Times)/NumTrials) / 1000000, 2), sigfigs(lists:max(Times) / 1000000, 2), sigfigs(lists:min(Times) / 1000000, 2)]),
    io:format("Lookup misses ~p~n~n", [lists:sum(Misses)]),
    rocksdb:close(DB),
    erlang:garbage_collect(),
    ok.

trial(DB, {Device, OUI}, DeviceMapping) ->
    DevAddr = maps:get(Device, DeviceMapping),

    {Time, Destination} = timer:tc(fun() ->
                                           {ok, Itr} = rocksdb:iterator(DB, []),
                                           {ok, _Key, <<Dest:32/integer-unsigned-big>>} = rocksdb:iterator_move(Itr, {seek_for_prev, <<DevAddr:32/integer-unsigned-big>>}),
                                           rocksdb:iterator_close(Itr),
                                           Dest
                                   end),
    %io:format("Device ~.16b was found in ~s seconds, found correct OUI ~p~n", [Device, sigfigs(Time / 1000000, 2), OUI == Destination]),
    case OUI == Destination of
        true ->
            {0, Time};
        false ->
            {1, Time}
    end.




allocation_size(Length) ->
    case Length of
        L when L =< 64 -> 64;
        L when L =< 128 -> 128;
        L when L =< 256 -> 256;
        L when L =< 512 -> 512;
        L when L =< 1024 -> 1024;
        L when L =< 2048 -> 2048;
        L when L =< 4096 -> 4096;
        L when L =< 8192 -> 8192;
        L when L =< 16384 -> 16384;
        L when L =< 32768 -> 32768;
        L when L =< 65536 -> 65536;
        L when L =< 131072 -> 131072
    end.

%% this is a lazy hack, but it does the job
sigfigs(Float, NumFigs) ->
    sigfigs(Float, NumFigs, 1).

sigfigs(0.0, _, _) ->
    "0";
sigfigs(Float, NumFigs, Precision) ->
    S = lists:flatten(io_lib:format("\~."++ integer_to_list(Precision) ++ "f", [Float])),
    case length(string:strip(S, both, $0)) >= NumFigs of
        true ->
            S;
        false ->
            sigfigs(Float, NumFigs, Precision+1)
    end.
