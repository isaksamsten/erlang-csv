erlang-csv
==========

Simple csv parser for erlang

Usage

```
Parser = csv:binary_reader("file.csv", [{annotation, true}]),
case csv:next_line(Parser) of
     {row, Line, Id} ->
     	io:format("~p: ~p ~n", [Id, Line]);
     {annotation, {Key, Value}} ->
          io:format("~p: ~p ~n", [Key, Value]);
     eof ->
     	 io:format("End of csv-file ~n")
end
```

TODO: clean up code and improve (to e.g. allow custom separators, etc)
