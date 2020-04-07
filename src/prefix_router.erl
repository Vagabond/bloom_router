-module(prefix_router).

-export([start/2]).

-define(BITS_23, 8388607).
-define(BITS_25, 33554431).

start(NumOUIs, NumDevices) ->
    io:format("~nrunning with ~p OUIs and ~p Devices~n", [NumOUIs, NumDevices]),
    Path = lists:flatten(io_lib:format("/tmp/prefix_router-~b-~b.db", [NumOUIs, NumDevices])),
    Options = [{create_if_missing, true}],
    {ok, DB} = rocksdb:open(Path, Options),
    RandomDevices = case rocksdb:get(DB, <<281474976710655:48/integer-unsigned-big, "randomdevices">>, []) of
        not_found ->
            io:format("generating OUIs~n"),
            NumTrials  = 1000,
            OUIs = lists:seq(1, NumOUIs),
            io:format("generated ~p OUIs~n", [NumOUIs]),
            io:format("generating ~p devices~n", [NumDevices]),
            DevicesWithOUIs = [ {rand:uniform(4294967296), rand:uniform(NumOUIs)} || _ <- lists:seq(1, NumDevices) ],
            DeviceMapping = lists:foldl(fun(OUI, Map) ->
                                  DevicesForThisOUI = [Device || {Device, OUI0} <- DevicesWithOUIs, OUI0 == OUI ],
                                  Size = allocation_size(length(DevicesForThisOUI)),
                                  {ok, Allocations} = get_allocation(DB, Size),
                                  %io:format("Allocations ~w~n", [Allocations]),
                                  Addresses = range(Allocations, []),
                                  %io:format("Addresses ~w~n", [Addresses]),
                                  [ rocksdb:put(DB, Allocation, <<OUI:32/integer-unsigned-big>>, []) || Allocation <- Allocations],
                                  maps:merge(Map, maps:from_list(lists:zip(DevicesForThisOUI, lists:sublist(Addresses, length(DevicesForThisOUI)))))
                          end, #{}, OUIs),
            io:format("compacting DB~n"),
            rocksdb:compact_range(DB, undefined, undefined, []),
            timer:sleep(5000),
            io:format("Attempting ~p random lookups~n", [NumTrials]),
            RD = [ lists:nth(rand:uniform(NumDevices), DevicesWithOUIs) || _ <- lists:seq(1, NumTrials)],
            rocksdb:put(DB, <<281474976710655:48/integer-unsigned-big, "randomdevices">>, term_to_binary(RD), []),
            rocksdb:put(DB, <<281474976710655:48/integer-unsigned-big, "mapping">>, term_to_binary(DeviceMapping), []),
            RD;
        {ok, BRD} ->
            RD = binary_to_term(BRD),
            NumTrials = length(RD),
            {ok, BMapping} = rocksdb:get(DB, <<281474976710655:48/integer-unsigned-big, "mapping">>, []),
            DeviceMapping = binary_to_term(BMapping),
            io:format("Attempting ~p random lookups~n", [NumTrials]),
            RD
    end,

    Parent = self(),
    Fun = fun F({Min, Max, Avg}=Acc) ->
    receive done ->
                Parent ! {memory, Acc}
    after 1 ->
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
    [Size] = rocksdb:get_approximate_sizes(DB, [{<<0:48/integer-unsigned-big>>, <<281474976710655:48/integer-unsigned-big>>}], include_files),
    io:format("Approximate database size ~sMb~n", [sigfigs(Size/(1024*1024), 2)]),
    %io:format("Average errors ~s, max ~p, min ~p~n", [sigfigs(lists:sum(Errors)/NumTrials, 2), lists:max(Errors), lists:min(Errors)]),
    io:format("Average lookup ~ss, max ~ss, min ~ss~n", [sigfigs((lists:sum(Times)/NumTrials) / 1000000, 2), sigfigs(lists:max(Times) / 1000000, 2), sigfigs(lists:min(Times) / 1000000, 2)]),
    io:format("Lookup misses ~p/~p~n~n", [lists:sum(Misses), length(RandomDevices)]),
    rocksdb:close(DB),
    erlang:garbage_collect(),
    ok.

trial(DB, {Device, OUI}, DeviceMapping) ->
    DevAddr = maps:get(Device, DeviceMapping),

   % io:format("searching for devaddr ~p :: ~p~n", [DevAddr, <<DevAddr:25/integer-unsigned-big, 0:23/integer>>]),
    {Time, Destination} = timer:tc(fun() ->
                                           {ok, Itr} = rocksdb:iterator(DB, []),
                                           Dest = lookup(Itr, DevAddr, rocksdb:iterator_move(Itr, {seek_for_prev, <<DevAddr:25/integer-unsigned-big, ?BITS_23:23/integer>>})),
                                           catch rocksdb:iterator_close(Itr),
                                           Dest
                                   end),
    %io:format("Device ~.16b was found in ~s seconds, found correct OUI ~p~n", [Device, sigfigs(Time / 1000000, 2), OUI == Destination]),
    case OUI == Destination of
        true ->
            {0, Time};
        false ->
            io:format("MISS ~p ~p ~p~n", [DevAddr, Destination, OUI]),
            {1, Time}
    end.


lookup(Itr, DevAddr, {ok, <<Base:25/integer-unsigned-big, Mask:23/integer-unsigned-big>>, <<Dest:32/integer-unsigned-big>>}) ->
    %Size = mask_to_size(Mask),
    case (DevAddr band (Mask bsl 2)) == Base of
        true ->
            Dest;
        %false when DevAddr > Base + Size ->
            %error;
        false ->
            lookup(Itr, DevAddr, rocksdb:iterator_move(Itr, prev))
    end;
lookup(_, _, _) ->
    error.

allocation_size(0) -> [8];
allocation_size(Length) ->
    %% round to the nearest 8
    Nearest = ceil(Length / 8) * 8,
    NumBits = floor(math:log2(Nearest -1)+1),
    trunc(math:pow(2, NumBits)).

mask_to_size(Mask) ->
    (((Mask bxor ?BITS_23) bsl 2) + 2#11) + 1.

size_to_mask(Size) ->
    %io:format("Size ~p~n", [Size]),
    ?BITS_23 bxor ((Size bsr 2) - 1).

get_allocation(DB, Size) ->
    %io:format("asking for allocation of size ~p~n", [Size]),
    {ok, Itr} = rocksdb:iterator(DB, []),
    Res = case rocksdb:iterator_move(Itr, last) of
        {ok, <<ABase:25/integer-unsigned-big, AMask:23/integer-unsigned-big>>, _} ->
            ASize = mask_to_size(AMask),
            %% sanity check
            0 = (ASize - 1) band ABase,
            MaxSize = max(Size, ASize),
            case ABase + ASize + Size < ?BITS_25 of
                true ->
                    %% ok there's room at the end
                    %% check if we can do it contiguously
                    Mask = size_to_mask(Size),
                    ANewBase = ABase band (Mask bsl 2),
                    case ANewBase + (MaxSize * 2) < ?BITS_25 of
                        true ->
                            Size = mask_to_size(Mask),
                            Base = ANewBase + max(Size, ASize),
                            %io:format("allocating ~b (~b) ~.2b~n", [Base, Size, Mask]),
                            %% sanity check
                            0 = (Size - 1) band Base,
                            {ok, [<<Base:25/integer-unsigned-big, Mask:23/integer-unsigned-big>>]};
                        false ->
                            {error, partial_allocation}
                    end;
                false ->
                    {error, partial_allocation}
            end;
        {error, invalid_iterator} ->
            Mask = size_to_mask(Size),
            {ok, [<<0:25/integer-unsigned-big, Mask:23/integer-unsigned-big>>]};
        {error, _} = Error ->
            Error
    end,
    catch rocksdb:iterator_close(Itr),
    Res.

range([], Acc) ->
    Acc;
range([<<Base:25/integer-unsigned-big, Mask:23/integer-unsigned-big>>|T], Acc) ->
    range(T, Acc ++ lists:seq(Base, Base + mask_to_size(Mask))).

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
