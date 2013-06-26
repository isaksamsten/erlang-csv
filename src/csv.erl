%%
%% Very basic CSV parser of limited use.
%%
%% It can only handle one type of separators (,)
%% string nesting
%%
%% Author: Isak Karlsson <isak-kar@dsv.su.se>
%%
-module(csv).
-compile(export_all).

%%
%% Spawn a 
%%
reader(File) ->
    {csv_reader, spawn_link(?MODULE, spawn_parser, [File])}.

binary_reader(File) ->
    {csv_reader, spawn_link(?MODULE, spawn_binary_parser, [File])}.

spawn_binary_parser(File) ->
    case file:read_file(File) of
	{ok, Bin} ->
	    parse_binary_incremental(Bin, 1);
	_ ->
	    throw({error, file_not_found})
    end.

parse_binary_incremental(Bin, Counter) ->
    Eof = binary_eof(Bin),
    case Eof of
	true ->
	    receive
		{Any, Parent} when Any == more; Any == raw ->
		    Parent ! {eof, Parent},
		    parse_binary_incremental(<<>>, Counter + 1);
		{exit, Parent} ->
		    Parent ! exit
	    end;
	false ->
	    receive
		{more, Parent} ->
		    {Item, Rest} = parse_binary_line(Bin, <<>>, []),
		    Parent ! {ok, Parent, Item, Counter},
		    parse_binary_incremental(Rest, Counter + 1);
		{raw, Parent} ->
		    {Line, Rest} = binary_next_line(Bin, <<>>), %% note: only check for EOF
		    Parent ! {raw, Parent, Line, Counter},
		    parse_binary_incremental(Rest, Counter + 1);
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
	{ok, Self, Item, Id} ->
	    demonitor(Ref),
	    {ok, Item, Id};
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


parse_binary_line(Binary) ->
    parse_binary_line(Binary, <<>>, []).

parse_binary_line(<<$\n, Rest/binary>>, Str, Acc) ->
    Acc0 = case Str of
	       <<>> ->
		   Acc;
	       _ ->
		   [string:strip(binary_to_list(Str))|Acc]
	   end,
    {lists:reverse(Acc0), Rest};
parse_binary_line(<<>>, _, _Acc) ->
    {[], <<>>};
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
