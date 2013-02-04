%%
%% Very basic CSV parser of limited use.
%%
%% It can only handle one type of separators (,) and only one level of
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
		    parse_incremental(Io, Counter + 1)
	    end;
	eof ->
	    receive 
		{more, Parent} ->
		    Parent ! {eof, Parent},
		    parse_incremental(Io, Counter)			
	    end;
	{error, Reason} ->
	    throw({error, Reason})
    end.

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
    parse_string_item(R, Str, Acc);
parse_line([$,|R], Str, Acc) ->
    parse_line(R, [], [string:strip(lists:reverse(Str))|Acc]);
parse_line([I|R], Str, Acc) ->
    parse_line(R, [I|Str], Acc).

parse_string_item([$", $,|R], Str, Acc) ->
    parse_line(R, [], [string:strip(lists:reverse(Str))|Acc]);
parse_string_item([$"|R], Str, Acc) ->
    parse_line(R, [], [string:strip(lists:reverse(Str))|Acc]);
parse_string_item([I|R], Str, Acc) ->
    parse_string_item(R, [I|Str], Acc).
