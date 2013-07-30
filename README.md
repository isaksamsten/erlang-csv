erlang-csv
==========

Simple csv parser for erlang

Usage

```
Parser = csv:reader("file.csv"),
case csv:next_line(Parser) of
     {ok, Line, Id} ->
     	 io:format("~p: ~p ~n", [Id, Line]);
     eof ->
     	 io:format("End of csv-file ~n")
end
```

TODO: clean up code and improve (to e.g. allow custom separators, etc)
