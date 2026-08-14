import os, subprocess, sys

APP = r'''
import os
from warmpy import warm_parse

@warm_parse
def main(
    msg:str='hi',  # What to say, or a mode: stdin, cwd, env, ppid, boom
    code:int=0,    # Exit code to return
    sleep:float=0, # Seconds to sleep before answering
):
    "warmpy test app"
    import sys, time
    if sleep:
        print('sleeping', flush=True)
        time.sleep(sleep)
    if msg == 'stdin': msg = sys.stdin.read().strip()
    elif msg == 'cwd': msg = os.getcwd()
    elif msg == 'env': msg = os.environ.get('WARMTEST', '')
    elif msg == 'ppid': msg = str(os.getppid())
    elif msg == 'boom': raise ValueError('boom')
    print(f'{msg} pid={os.getpid()}')
    return code
'''

def run(tmp, *args, env=None, input=None, cwd=None):
    e = dict(os.environ, PYTHONPATH=str(tmp), XDG_RUNTIME_DIR=str(tmp/'run'))
    if env: e.update(env)
    return subprocess.run([sys.executable, '-c', 'import sys; from app import main; sys.exit(main())', *args],
        capture_output=True, text=True, env=e, timeout=30, input=input, cwd=cwd)

def pid_of(r): return int(r.stdout.rsplit('pid=', 1)[1])

def test_warmpy(tmp_path):
    (tmp_path/'app.py').write_text(APP)
    (tmp_path/'run').mkdir()
    try:
        r1 = run(tmp_path)
        assert r1.returncode==0, r1.stderr
        assert r1.stdout.startswith('hi pid=')
        r2 = run(tmp_path)
        assert pid_of(r2)==pid_of(r1)                    # second call reused the warm server
        r3 = run(tmp_path, '--msg', 'bye', '--code', '3')
        assert r3.returncode==3 and r3.stdout.startswith('bye')   # args and exit code pass through
        assert pid_of(r3)==pid_of(r1)
        rc = run(tmp_path, env={'WARMPY': '0'})
        assert rc.returncode==0 and pid_of(rc)!=pid_of(r1)        # WARMPY=0 runs in the client process
        rs = run(tmp_path, '--warmpy-stop')
        assert rs.returncode==0
        r4 = run(tmp_path)
        assert pid_of(r4)!=pid_of(r1)                    # stop worked: a fresh server took over
    finally: run(tmp_path, '--warmpy-stop')


def popen(tmp, *args):
    e = dict(os.environ, PYTHONPATH=str(tmp), XDG_RUNTIME_DIR=str(tmp/'run'))
    return subprocess.Popen([sys.executable, '-c', 'import sys; from app import main; sys.exit(main())', *args],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=e)

def test_concurrent(tmp_path):
    (tmp_path/'app.py').write_text(APP)
    (tmp_path/'run').mkdir()
    try:
        run(tmp_path)                                    # warm up: supervisor and first worker
        ps = [popen(tmp_path, '--sleep', '0.6') for _ in range(2)]
        outs = [p.communicate(timeout=15) for p in ps]
        assert all(p.returncode==0 for p in ps), outs
        pids = {o[0].rsplit('pid=', 1)[1] for o in outs}
        assert len(pids)==2   # a second worker spawns only when the first is busy, so two pids proves concurrency
    finally: run(tmp_path, '--warmpy-stop')


def test_transparency(tmp_path):
    (tmp_path/'app.py').write_text(APP)
    (tmp_path/'run').mkdir()
    sub = tmp_path/'sub'
    sub.mkdir()
    try:
        base = pid_of(run(tmp_path))
        r = run(tmp_path, '--msg', 'stdin', input='knock knock')
        assert r.stdout.startswith('knock knock ') and pid_of(r)==base   # stdin reaches the worker
        r = run(tmp_path, '--msg', 'cwd', cwd=sub)
        assert r.stdout.startswith(str(sub)) and pid_of(r)==base         # caller's cwd, not the server's
        r = run(tmp_path, '--msg', 'env', env={'WARMTEST': 'zap'})
        assert r.stdout.startswith('zap ') and pid_of(r)==base           # caller's env, not the server's
        r = run(tmp_path, '--msg', 'boom')
        assert r.returncode==1 and 'ValueError: boom' in r.stderr and 'pid=' not in r.stdout
        assert pid_of(run(tmp_path))==base                               # the worker survived the exception
    finally: run(tmp_path, '--warmpy-stop')


def test_crash_recovery(tmp_path):
    import signal, time
    (tmp_path/'app.py').write_text(APP)
    (tmp_path/'run').mkdir()
    try:
        w1 = pid_of(run(tmp_path))
        os.kill(w1, signal.SIGKILL)                          # kill the worker; the supervisor lives
        r = run(tmp_path)
        assert r.returncode==0 and pid_of(r)!=w1             # a fresh worker serves
        sup = int(run(tmp_path, '--msg', 'ppid').stdout.split(' ')[0])
        os.kill(sup, signal.SIGKILL)                         # kill the supervisor: the socket file is left behind
        time.sleep(0.2)
        assert list((tmp_path/'run').glob('*.sock'))         # stale socket present
        assert run(tmp_path).returncode==0                   # recovered: unlink and respawn
    finally: run(tmp_path, '--warmpy-stop')


def test_mismatch(tmp_path):
    import hashlib, socket, struct, threading
    (tmp_path/'app.py').write_text(APP)
    rundir = tmp_path/'run'
    rundir.mkdir()
    h = hashlib.sha1(f'app:main|{sys.executable}'.encode()).hexdigest()[:16]
    spath = rundir/f'warmpy-{h}.sock'
    srv = socket.socket(socket.AF_UNIX)
    old = os.getcwd()
    os.chdir(rundir)
    try: srv.bind(spath.name)
    finally: os.chdir(old)
    srv.listen()
    got = {}
    def fake():
        conn, _ = srv.accept()
        with conn:
            conn.sendall(struct.pack('!I', 5) + b'WRONG')
            n = struct.unpack('!I', conn.recv(4))[0]
            got['req'] = conn.recv(n)
            spath.unlink()      # a real outdated server unlinks on its way out
        srv.close()
    threading.Thread(target=fake, daemon=True).start()
    try:
        r = run(tmp_path)
        assert r.returncode==0 and 'hi pid=' in r.stdout     # a real server took over
        assert b'stop' in got['req']                         # the impostor was told to stop
    finally: run(tmp_path, '--warmpy-stop')


def test_interrupt(tmp_path):
    import signal
    (tmp_path/'app.py').write_text(APP)
    (tmp_path/'run').mkdir()
    try:
        base = pid_of(run(tmp_path))
        p = popen(tmp_path, '--sleep', '10')
        assert p.stdout.readline().startswith('sleeping')    # the request is running in the worker
        p.send_signal(signal.SIGINT)
        p.communicate(timeout=5)
        assert p.returncode==130
        assert pid_of(run(tmp_path))==base                   # the worker survived the interrupt
    finally: run(tmp_path, '--warmpy-stop')


def test_burst(tmp_path):
    (tmp_path/'app.py').write_text(APP)
    (tmp_path/'run').mkdir()
    try:
        ps = [popen(tmp_path) for _ in range(5)]             # no server yet: bind race, then queueing
        outs = [p.communicate(timeout=20) for p in ps]
        assert all(p.returncode==0 for p in ps), outs
        pids = {o[0].rsplit('pid=', 1)[1].strip() for o in outs}
        assert 1 <= len(pids) <= 4                           # served by at most `cap` workers
    finally: run(tmp_path, '--warmpy-stop')


def test_workers_kwarg(tmp_path):
    (tmp_path/'app.py').write_text(APP.replace('@warm_parse', '@warm_parse(workers=1)'))
    (tmp_path/'run').mkdir()
    try:
        run(tmp_path)
        ps = [popen(tmp_path, '--sleep', '0.4') for _ in range(2)]
        outs = [p.communicate(timeout=15) for p in ps]
        assert all(p.returncode==0 for p in ps), outs
        pids = {o[0].rsplit('pid=', 1)[1].strip() for o in outs}
        assert len(pids)==1                                  # workers=1 serializes onto one worker
    finally: run(tmp_path, '--warmpy-stop')


def test_once(tmp_path):
    (tmp_path/'app.py').write_text(APP)
    (tmp_path/'run').mkdir()
    r = run(tmp_path, '--msg', 'solo', '--warmpy-once')
    assert r.returncode==0, r.stderr
    assert r.stdout.startswith('solo pid=')
    assert not list((tmp_path/'run').iterdir())   # ran in-process: no socket, no server, no log
