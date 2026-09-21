"Run CLI functions in a background process that keeps slow imports loaded."
import asyncio, hashlib, inspect, json, os, signal, socket, struct, subprocess, sys, threading, time, traceback
from contextlib import suppress
from functools import partial, wraps
from importlib import import_module, metadata
from pathlib import Path
from fastcore.script import parse_cli
from fastcore.foundation import working_directory
from fastcore.xtras import trace

__all__ = ['warm_parse']

def _sockpath(target):
    d = os.environ.get('XDG_RUNTIME_DIR')
    d = Path(d) if d else Path.home()/'.cache'/'warmpy'
    d.mkdir(mode=0o700, parents=True, exist_ok=True)
    h = hashlib.sha1(f'{target}|{sys.executable}'.encode()).hexdigest()[:16]
    return d/f'warmpy-{h}.sock'

def _token(target):
    top = target.split(':')[0].split('.')[0]
    try: pv = metadata.version(top)
    except Exception: pv = ''
    from warmpy import __version__
    return f'{__version__}|{top}={pv}|{sys.executable}'

def _send_frame(s, data): s.sendall(struct.pack('!I', len(data)) + data)

def _recvall(s, n):
    buf = b''
    while len(buf) < n:
        got = s.recv(n - len(buf))
        if not got: raise ConnectionError('peer closed mid-frame')
        buf += got
    return buf

def _recv_frame(s): return _recvall(s, struct.unpack('!I', _recvall(s, 4))[0])

# --- client ---

def _in_sockdir(path):
    "chdir to the socket's directory, so AF_UNIX names stay under the OS path-length cap."
    return working_directory(path.parent)

def _try_connect(path, patience=1.0):
    "Connect to `path`. A refusal can be a server between bind and listen, so only unlink as stale after `patience` seconds."
    deadline = time.monotonic() + patience
    while True:
        s = socket.socket(socket.AF_UNIX)
        try:
            with _in_sockdir(path): s.connect(path.name)
            return s
        except FileNotFoundError: return None
        except OSError:
            if time.monotonic() >= deadline:
                with suppress(OSError): path.unlink()
                return None
            time.sleep(0.05)

def _spawn(target, path, idle, workers):
    with open(path.with_suffix('.log'), 'ab') as logf:
        subprocess.Popen([sys.executable, '-m', 'warmpy', target, str(path), str(idle), str(workers)],
            stdin=subprocess.DEVNULL, stdout=logf, stderr=logf, start_new_session=True)

def _await_server(path, tries=50):
    for _ in range(tries):
        s = _try_connect(path)
        if s is not None: return s
        time.sleep(0.1)
    return None

def _request(s, args, kw):
    req = json.dumps(dict(op='call', args=args, kw=kw, cwd=os.getcwd(), env=dict(os.environ))).encode()
    socket.send_fds(s, [struct.pack('!I', len(req))], [0, 1, 2])
    s.sendall(req)
    old = signal.signal(signal.SIGINT, lambda *a: s.sendall(b'\x03'))
    try: return json.loads(_recv_frame(s))['exit']
    finally: signal.signal(signal.SIGINT, old)

def _warm_call(func, target, args, kw, idle, workers):
    try:
        path, tok = _sockpath(target), _token(target)
        for attempt in (1, 2):
            s = _try_connect(path)
            if s is None:
                _spawn(target, path, idle, workers)
                s = _await_server(path)
                if s is None: break
            with s:
                if _recv_frame(s).decode() != tok:
                    _send_frame(s, json.dumps(dict(op='stop')).encode())
                    with suppress(Exception): _recv_frame(s)
                    continue
                return _request(s, args, kw)
    except Exception: pass
    return _cold_call(func, args, kw)  # any warmpy failure: run it here, slowly

def _cold_call(func, args, kw):
    try:
        res = func(*args, **kw)
        if inspect.isawaitable(res): res = asyncio.run(res)
        return res
    except KeyboardInterrupt: return 130

def _stop_server(target):
    s = _try_connect(_sockpath(target))
    if s is None: return 0  # no server: already stopped
    with s:
        _recv_frame(s)
        _send_frame(s, json.dumps(dict(op='stop')).encode())
        with suppress(Exception): _recv_frame(s)
    return 0

# --- server ---

def _watch(conn, active):
    "Interrupt the running call if the client sends SIGINT or vanishes."
    with suppress(Exception):
        while active.is_set():
            b = conn.recv(1)
            if not active.is_set(): return
            if not b or b == b'\x03':
                os.kill(os.getpid(), signal.SIGINT)
                return

def _exit_code(e):
    if e.code is None: return 0
    if isinstance(e.code, int): return e.code
    print(e.code, file=sys.stderr)
    return 1

def _handle(conn, func):
    "Run one request from `conn` on the client's own stdio, cwd, and env; True means the client asked us to stop."
    buf, fds, flags, addr = socket.recv_fds(conn, 4, 3)
    n = struct.unpack('!I', buf + _recvall(conn, 4 - len(buf)))[0] if len(buf) < 4 else struct.unpack('!I', buf)[0]
    req = json.loads(_recvall(conn, n))
    if req['op'] == 'stop':
        _send_frame(conn, b'{"exit": 0}')
        return True
    saved = [os.dup(i) for i in (0, 1, 2)]
    for i, fd in enumerate(fds):
        os.dup2(fd, i)
        os.close(fd)
    os.chdir(req['cwd'])
    os.environ.clear()
    os.environ.update(req['env'])
    active = threading.Event()
    active.set()
    threading.Thread(target=_watch, args=(conn, active), daemon=True).start()
    code = 0
    try:
        res = func(*req['args'], **req['kw'])
        if inspect.isawaitable(res): res = asyncio.run(res)
        if isinstance(res, int): code = res
    except SystemExit as e: code = _exit_code(e)
    except KeyboardInterrupt: code = 130
    except Exception:
        traceback.print_exc()
        code = 1
    finally:
        active.clear()
        sys.stdout.flush()
        sys.stderr.flush()
        for i, fd in enumerate(saved):
            os.dup2(fd, i)
            os.close(fd)
    _send_frame(conn, json.dumps(dict(exit=code)).encode())
    return False

def work(target, ctlfd, idle):
    "Worker: import `target` once, then serve connections handed over `ctlfd` one at a time."
    mod, qual = target.split(':')
    func = getattr(import_module(mod), qual)
    func = getattr(func, '__wrapped__', func)
    ctl = socket.socket(fileno=ctlfd)
    while True:
        ctl.settimeout(idle)
        try: buf, fds, flags, addr = socket.recv_fds(ctl, 1, 1)
        except TimeoutError: break
        if not buf or not fds: break
        conn = socket.socket(fileno=fds[0])
        stop = False
        with conn, suppress(Exception): stop = _handle(conn, func)
        with suppress(OSError): ctl.sendall(b'S' if stop else b'D')
        if stop: break

class _Worker:
    def __init__(self, target, idle):
        self.ctl, wrk = socket.socketpair()
        self.proc = subprocess.Popen([sys.executable, '-m', 'warmpy', '--worker', target, str(wrk.fileno()), str(idle)],
            pass_fds=[wrk.fileno()], stdin=subprocess.DEVNULL)
        wrk.close()
        self.busy,self.conn = False,None

    def give(self, conn):
        "Hand `conn` to the worker. It reads `conn` when its import is done. Keep this copy open until `done`. macOS breaks a connection that is closed while in transit."
        socket.send_fds(self.ctl, [b'C'], [conn.fileno()])
        self.busy,self.conn = True,conn

    def done(self):
        "Close the supervisor's copy of a connection the worker has finished with"
        self.busy = False
        if self.conn: self.conn.close()

def supervise(target, path, idle, cap=4):
    "Own the socket, never import the app, and hand each connection to an idle worker, spawning up to `cap` of them."
    import selectors
    path = Path(path)
    sock = socket.socket(socket.AF_UNIX)
    try:
        with _in_sockdir(path): sock.bind(path.name)
    except OSError: return  # another supervisor won the race
    sock.listen()
    tok = _token(target).encode()
    sel = selectors.DefaultSelector()
    sel.register(sock, selectors.EVENT_READ, None)
    workers, pending, stopping = [], [], False

    def drop(w):
        sel.unregister(w.ctl)
        w.ctl.close()
        w.done()
        workers.remove(w)

    def dispatch(conn):
        w = next((o for o in workers if not o.busy), None)
        if w is None:
            if len(workers) >= cap:
                pending.append(conn)
                return
            w = _Worker(target, idle)
            workers.append(w)
            sel.register(w.ctl, selectors.EVENT_READ, w)
        try: w.give(conn)
        except OSError:
            drop(w)
            dispatch(conn)

    try:
        while True:
            events = sel.select(timeout=idle)
            if not events and not any(w.busy for w in workers): break
            for key, _ in events:
                if key.data is None:
                    conn, _ = sock.accept()
                    if stopping:
                        conn.close()
                        continue
                    _send_frame(conn, tok)
                    dispatch(conn)
                else:
                    w = key.data
                    b = b''
                    with suppress(OSError): b = w.ctl.recv(1)
                    if b == b'S':
                        stopping = True
                        w.done()
                    elif b == b'D':
                        w.done()
                        if pending: w.give(pending.pop(0))
                    else: drop(w)  # EOF: the worker timed out or died
            if stopping and not any(w.busy for w in workers) and not pending: break
            if not workers and stopping: break
    finally:
        with suppress(OSError): Path(path).unlink()
        for w in workers: w.ctl.close()  # EOF tells idle workers to exit

# --- decorator ---

def _pop_flag(flag):
    "Remove `flag` from `sys.argv`, returning whether it was there"
    if flag not in sys.argv: return False
    sys.argv.remove(flag)
    return True

def warm_parse(func=None, *, idle=1800, workers=4, pos=None):
    "Like `fastcore.script.call_parse`, but the function body runs in a warm background process."
    if func is None: return partial(warm_parse, idle=idle, workers=workers, pos=pos)
    target = f'{func.__module__}:{func.__qualname__}'
    @wraps(func)
    def _f(*args, **kwargs):
        if args or kwargs: return func(*args, **kwargs)
        once,stop = _pop_flag('--warmpy-once'),_pop_flag('--warmpy-stop')
        if stop: return _stop_server(target)
        pargs,pa,pdb = parse_cli(func, pos=pos)
        if pdb: return _cold_call(trace(func), pargs, pa)
        if once or os.environ.get('WARMPY') == '0' or not hasattr(socket, 'send_fds'): return _cold_call(func, pargs, pa)
        return _warm_call(func, target, pargs, pa, idle, workers)
    return _f
