# Gitaly code style

## Errors

### Use %v when wrapping errors

Use `%v` when wrapping errors with context.

    fmt.Errorf("foo context: %v", err)

### Keep errors short

It is customary in Go to pass errors up the call stack and decorate
them. To be a good neighbor to the rest of the call stack we should keep
our errors short.

    // Good
    fmt.Errorf("peek diff line: %v", err)

    // Too long
    fmt.Errorf("ParseDiffOutput: Unexpected error while peeking: %v", err)

### Use lower case in errors

Use lower case in errors; it is OK to preserve upper case in names.

### Errors should stick to the facts

It is tempting to write errors that explain the problem that occurred.
This can be appropriate in some end-user facing situations, but it is
never appropriate for internal error messages. When your
interpretation is wrong it puts the reader on the wrong track.

Stick to the facts. Often it is enough to just describe in a few words
what we were trying to do.

### Use %q when interpolating strings

Unless it would lead to incorrect results, always use `%q` when
interpolating strings. The `%q` operator quotes strings and escapes
spaces and non-printable characters. This can save a lot of debugging
time.

## Return statements

### Don't use "naked return"

In a function with named return variables it is valid to have a plain
("naked") `return` statement, which will return the named return
variables.

In Gitaly we don't use this feature. If the function returns one or
more values, then always pass them to `return`.

## Tests

### Table-driven tests

We like table-driven tests ([Table-driven tests using subtests](https://blog.golang.org/subtests#TOC_4.), [Cheney blog post], [Golang wiki]).

-   Use [subtests](https://blog.golang.org/subtests#TOC_4.) with your table-driven tests, using `t.Run`:

```
func TestTime(t *testing.T) {
    testCases := []struct {
        gmt  string
        loc  string
        want string
    }{
        {"12:31", "Europe/Zuri", "13:31"},
        {"12:31", "America/New_York", "7:31"},
        {"08:08", "Australia/Sydney", "18:08"},
    }
    for _, tc := range testCases {
        t.Run(fmt.Sprintf("%s in %s", tc.gmt, tc.loc), func(t *testing.T) {
            loc, err := time.LoadLocation(tc.loc)
            if err != nil {
                t.Fatal("could not load location")
            }
            gmt, _ := time.Parse("15:04", tc.gmt)
            if got := gmt.In(loc).Format("15:04"); got != tc.want {
                t.Errorf("got %s; want %s", got, tc.want)
            }
        })
    }
}
```

  [Cheney blog post]: https://dave.cheney.net/2013/06/09/writing-table-driven-tests-in-go
  [Golang wiki]: https://github.com/golang/go/wiki/TableDrivenTests
