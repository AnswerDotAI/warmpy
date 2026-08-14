# warmpy

Importing a big library takes seconds. Python pays that cost once per process, and a CLI starts a new process for every command, so a CLI pays it on every command. warmpy pays it once. The wrapped function runs in a background process that has already done its imports. The command you type becomes a small program that starts fast, sends its arguments to the background process, and shows the output.

PersistentPerl solved this problem for Perl CLIs twenty years ago, under the name `pperl`. warmpy is the same idea for Python: a background process that starts on first use, serves later calls, and exits when idle.

## Install

    pip install warmpy

warmpy needs unix sockets that can pass file descriptors, which macOS and Linux provide. Where they are absent, every call runs in the calling process, slowly, with the same results.

## Use

    from warmpy import warm_parse

    @warm_parse
    def main(
        path:str=None,  # File to process; stdin if omitted
    ):
        "Process a file"
        from .core import process   # the slow import goes inside the body
        ...

`warm_parse` takes the place of `fastcore.script.call_parse`. The function signature and its docments define the command line. Parsing happens in the calling process, so `--help` and argument errors never touch the background process. `warm_parse(idle=1800, workers=4)` sets how many seconds of disuse end the background process and how many worker processes may exist at once.

The module that holds the wrapped function must import fast. Heavy imports go inside the function body. A slow module makes every call slow, and warmpy cannot fix that.

## The rule

Running through the background process changes nothing except speed. The function gets the same arguments, reads the same stdin, writes to the same terminal, sees the same working directory and the same environment variables, and produces the same exit code. Ctrl-C still stops it. The command hands its own stdin, stdout, and stderr to the background process, and sends its current directory and environment on every call. If the background process is missing or broken, the command imports the library and runs the function itself.

## Processes

The first call starts a supervisor and one worker. The supervisor owns the socket and never imports your code. Each worker imports your code once, then serves one request at a time. When a request arrives and every worker is busy, the supervisor starts another worker, up to `workers` of them. After `idle` seconds without requests, the workers and the supervisor exit. Nobody manages these processes by hand.

## Failures

Every failure has a fixed recovery, and none of them reaches the user. If the socket file exists but nothing answers for a second, the command deletes the file and starts fresh. If the background process was built from an older version of the package, the command tells it to exit and starts fresh; every connection begins with a version check. If two commands race to start the background process, one wins and the other connects to the winner. If a worker dies mid-request, that command falls back to running the function itself. The worst outcome warmpy permits is a slow one.

## Stale code

The version check compares package versions, so an editable install that changed on disk is not detected. The background process keeps serving the code it imported. `yourcommand --warmpy-stop` ends it, and the next call starts fresh with the current code. `yourcommand --warmpy-once` (or `WARMPY=0 yourcommand`) runs one call in the current process and never starts or contacts the background process, which also makes it the right form for tests.

## The function's obligations

The function must not depend on module-level variables it changed during earlier calls. In a new process those changes are gone. In the background process they persist. warmpy cannot see the difference.

## Files

The socket and a log of background-process errors live under `$XDG_RUNTIME_DIR`, or `~/.cache/warmpy` when that is unset. Each combination of function, Python executable, and package version gets its own socket.
