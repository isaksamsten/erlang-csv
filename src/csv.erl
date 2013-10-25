%%
%% Very basic CSV parser of limited use.
%%
%% It can only handle one type of separators (,)
%% and simple strings
%% 
%% Version 0.2:
%%   - comment (line starting with %) and 
%%     annotation support (line starting with %@key value)
%% Version 0.1:
%%   - initial release
%% Author: Isak Karlsson <isak-kar@dsv.su.se>
%%
-module(csv).
-compile(export_all).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.


reader(File) ->
    {?MODULE, {csv_reader, spawn_link(?MODULE, spawn_parser, [File])}}.

binary_reader(File, Opts) ->
    Annotations = proplists:get_value(annotations, Opts, false),
    {?MODULE, {csv_reader, spawn_link(?MODULE, spawn_binary_parser, [File, Annotations])}}.
binary_reader(File) ->
    binary_reader(File, []).

spawn_binary_parser(File, Annot) ->
    case file:read_file(File) of
        {ok, Bin} ->
            parse_binary_incremental(Bin, 1, Annot);
        _ ->
            throw({error, file_not_found})
    end.

parse_binary_incremental(Bin, Counter, Annot) ->
    Eof = binary_eof(Bin),
    case Eof of
        true ->
            receive
                {Any, Parent} when Any == more; Any == raw ->
                    Parent ! {eof, Parent},
                    parse_binary_incremental(<<>>, Counter + 1, Annot);
                {exit, Parent} ->
                    Parent ! exit
            end;
        false ->
            receive
                {more, Parent} ->
                    case parse_binary_line(Bin, Annot) of
                        {item, {Item, Rest}} ->
                            Parent ! {row, Parent, Item, Counter},
                            parse_binary_incremental(Rest, Counter + 1, Annot);
                        {annotation, {Kv, Rest}} ->
                            Parent ! {annotation, Parent, Kv},
                            parse_binary_incremental(Rest, Counter, Annot)
                    end;
                {raw, Parent} ->
                    {Line, Rest} = binary_next_line(Bin, <<>>), %% note: only check for EOF
                    Parent ! {raw, Parent, Line, Counter},
                    parse_binary_incremental(Rest, Counter + 1, Annot);
                {exit, Parent} ->
                    Parent ! exit
            end 
    end.

spawn_parser(File) ->
    case file:open(File, [read, read_ahead]) of
        {ok, Io} ->
            parse_incremental(Io, 1);
        _ ->
            throw({error, file_not_found})
    end.

parse_incremental(Io, Counter) ->
    case file:read_line(Io) of
        {ok, Line} ->
            Item = parse_line(Line, []),
            receive
                {more, Parent} ->
                    Parent ! {ok, Parent, Item, Counter},
                    parse_incremental(Io, Counter + 1);
                {exit, Parent} ->
                    Parent ! exit
            end;
        eof ->
            receive 
                {more, Parent} ->
                    Parent ! {eof, Parent},
                    parse_incremental(Io, Counter);
                {exit, Parent} ->
                    Parent ! exit
            end;
        {error, Reason} ->
            throw({error, Reason})
    end.

kill({csv_reader, Pid}) ->
    Ref = monitor(process, Pid),
    Pid ! {exit, self()},
    receive
        exit ->
            ok;
        {'DOWN', Ref, _, _, _} ->
            demonitor(Ref),
            ok
    end.    

next_line(Reader) ->
    get_next_line(Reader).

get_next_line({csv_reader, Pid}) ->
    Self = self(),
    Ref = monitor(process, Pid),
    Pid ! {more, Self},
    receive
        {row, Self, Item, Id} ->
            demonitor(Ref),
            {row, Item, Id};
        {annotation, Self, Kv} ->
            demonitor(Ref),
            {annotation, Kv};
        {eof, Self} ->
            demonitor(Ref),
            eof;
        {'DOWN', Ref, _, _, _} ->
            demonitor(Ref),
            eof         
    end.

get_next_raw({csv_reader, Pid}) ->
    Self = self(),
    Ref = monitor(process, Pid),
    Pid ! {raw, Self},
    receive
        {raw, Self, Item, Id} ->
            demonitor(Ref),
            {ok, Item, Id};
        {eof, Self} ->
            demonitor(Ref),
            eof;
        {'DOWN', Ref, _, _, _} ->
            demonitor(Ref),
            eof
    end.

binary_eof(<<>>) ->
    true;
binary_eof(_) ->
    false.


binary_next_line(<<>>, _) ->
    {eof, <<>>};
binary_next_line(<<$\n, Rest/binary>>, Acc) ->
    {<<Acc/binary, $\n>>, Rest};
binary_next_line(<<$\r, Rest/binary>>, Acc) -> %% NOTE: skip \r
    binary_next_line(Rest, Acc);
binary_next_line(<<Any, Rest/binary>>, Acc) ->
    binary_next_line(Rest, <<Acc/binary, Any>>).


parse_binary_line(Binary, Annot) ->
    case parse_binary_line(Binary, <<>>, []) of
        {comment, Rest} ->
            parse_binary_line(Rest, Annot);
        {annotation, {Kv, Rest}} ->
            if not(Annot) ->
                    parse_binary_line(Rest, Annot);
               true ->
                    {annotation, {Kv, Rest}}
            end;
        Other ->
            {item, Other}
    end.

end_of_line(Rest, Str, Acc) ->
    Acc0 = case Str of
               <<>> ->
                   Acc;
                _ ->
                    [string:strip(binary_to_list(Str))|Acc]
           end,
    case Acc0 of
        [] ->
            {comment, Rest};
        FinalAcc ->
            {lists:reverse(FinalAcc), Rest}
    end.

parse_binary_line(<<$%, Annotation, Rest/binary>>, _, _) ->
    case is_annotation(Annotation) of
        true ->
            {annotation, parse_annotation(Rest)};
        false ->
            {comment, skip_line(Rest)}
    end;
parse_binary_line(<<$\n, Rest/binary>>, Str, Acc) ->
   end_of_line(Rest, Str, Acc);
parse_binary_line(<<>>, Str, Acc) ->
    end_of_line(<<>>, Str, Acc);
parse_binary_line(<<$\r, Rest/binary>>, Str, Acc) -> %% NOTE: skip \r
    parse_binary_line(Rest, Str, Acc);
parse_binary_line(<<$", $,, Rest/binary>>, _Str, Acc) ->
    parse_binary_line(Rest, <<>>, ["\""|Acc]);
parse_binary_line(<<$", Rest/binary>>, Str, Acc) ->
    parse_binary_string(Rest, Str, Acc);
parse_binary_line(<<$,, Rest/binary>>, Str, Acc) ->
    parse_binary_line(Rest, <<>>, [string:strip(binary_to_list(Str))|Acc]);
parse_binary_line(<<I, Rest/binary>>, Str, Acc) ->
    parse_binary_line(Rest, <<Str/binary, I>>, Acc).

parse_binary_string(<<$", $,, Rest/binary>>, Str, Acc) ->
    parse_binary_line(Rest, <<>>, [string:strip(binary_to_list(Str))|Acc]);
parse_binary_string(<<$", Rest/binary>>, Str, Acc) ->
    parse_binary_line(Rest, <<>>, [string:strip(binary_to_list(Str))|Acc]);
parse_binary_string(<<I, Rest/binary>>, Str, Acc) ->
    parse_binary_string(Rest, <<Str/binary, I>>, Acc).

skip_line(<<$\r, Rest/binary>>) ->
    skip_line(Rest);
skip_line(<<$\n, Rest/binary>>) ->
    Rest;
skip_line(<<_, Rest/binary>>) ->
    skip_line(Rest).

is_annotation(A) when A == $@ ->
    true;
is_annotation(_) ->
    false.

parse_annotation(A) ->
    {Key, Rest0} = parse_annotation_key(A, <<>>),
    {Value, Rest1} = parse_annotation_value(Rest0, <<>>),
    {{Key, Value}, Rest1}.

parse_annotation_key(<<$ , Rest/binary>>, Acc) ->
    {Acc, Rest};
parse_annotation_key(<<C, Rest/binary>>, Acc) ->
    parse_annotation_key(Rest, <<Acc/binary,C>>).

parse_annotation_value(<<$\r, Rest/binary>>, Acc) ->
    parse_annotation_value(Rest, Acc);
parse_annotation_value(<<$\n, Rest/binary>>, Acc) ->
    {Acc, Rest};
parse_annotation_value(<<C, Rest/binary>>, Acc) ->
    parse_annotation_value(Rest, <<Acc/binary,C>>).

parse_line(Line, Acc) ->
    lists:reverse(parse_line(Line, [], Acc)).
parse_line([End], Str, Acc) ->
    Str0 = case End of
               $\n ->
                   Str;
               _ -> % NOTE: The last line of the file
                   [End|Str]
           end, 
    case Str0 of
        [] ->
            Acc;
        _ ->
            [string:strip(lists:reverse(Str0))|Acc]
    end;
parse_line([$%|_], _, Acc) ->
    Acc;
parse_line([$", $,|R], _, Acc) ->
    parse_line(R, [], ["\""|Acc]);
parse_line([$"|R], Str, Acc) ->
    parse_string(R, Str, Acc);
parse_line([$,|R], Str, Acc) ->
    parse_line(R, [], [string:strip(lists:reverse(Str))|Acc]);
parse_line([I|R], Str, Acc) ->
    parse_line(R, [I|Str], Acc).

parse_string([$", $,|R], Str, Acc) ->
    parse_line(R, [], [string:strip(lists:reverse(Str))|Acc]);
parse_string([$"|R], Str, Acc) ->
    parse_line(R, [], [string:strip(lists:reverse(Str))|Acc]);
parse_string([I|R], Str, Acc) ->
    parse_string(R, [I|Str], Acc).

-ifdef(TEST).
annotation_test() ->
    {_, Csv} = binary_reader("../test/csv_comment.csv", [{annotations, true}]),
    {annotation, {Url, Http}} = next_line(Csv),
    ?assertEqual(<<"url">>, Url),
    ?assertEqual(<<"http://people.dsv.su.se/~isak-kar">>, Http),

    {annotation, {Name, TheName}} = next_line(Csv),
    ?assertEqual(<<"name">>, Name),
    ?assertEqual(<<"Hello World">>, TheName),

    {row, Line, Count} = next_line(Csv),
    ?assertEqual(["hello", "world"], Line),
    ?assertEqual(1, Count).

ignore_annotation_test() ->
    {_, Csv} = binary_reader("../test/csv_comment.csv", [{annotations, false}]),    
    {row, Line, Count} = next_line(Csv),
    ?assertEqual(["hello", "world"], Line),
    ?assertEqual(1, Count).

-endif.
