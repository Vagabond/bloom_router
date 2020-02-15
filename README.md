bloom_router
=====

    $ rebar3 compile
    erl -pa _build/default/lib/*/ebin -noshell -eval "bloom_router:start(xor8_xxhash, 10000, 8000000), bloom_router:start(xor16_xxhash, 10000, 8000000), bloom_router:start(bloom, 10000, 8000000), erlang:halt()" > run.log

You should do at least 2 runs, the first run will generate a database that will be used for subsequent runs with the same parameters.
