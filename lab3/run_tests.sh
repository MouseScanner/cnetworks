#!/bin/bash
set -e
cd "$(dirname "$0")"
make clean >/dev/null
make
./server &
PID=$!
cleanup() { kill $PID 2>/dev/null || true; wait $PID 2>/dev/null || true; }
trap cleanup EXIT
sleep 0.3
python3 - <<'PY'
import socket, struct, time

PORT = 9090

def send_msg(s, t, payload=b""):
    L = 1 + len(payload)
    s.sendall(struct.pack(">I", L) + bytes([t]) + payload)

def recv_msg(s):
    h = s.recv(4)
    if len(h) < 4:
        return None, None
    L = struct.unpack(">I", h)[0]
    t = s.recv(1)[0]
    pl = s.recv(L - 1) if L > 1 else b""
    return t, pl

def wait_port():
    for _ in range(50):
        try:
            x = socket.socket()
            x.connect(("127.0.0.1", PORT))
            x.close()
            return
        except OSError:
            time.sleep(0.05)
    raise SystemExit("server not up")

wait_port()

def open_client(nick):
    s = socket.socket()
    s.connect(("127.0.0.1", PORT))
    send_msg(s, 1, nick.encode())
    t, pl = recv_msg(s)
    assert t == 2, (t, pl)
    return s

s1 = open_client("Client1")
s2 = open_client("Client2")
send_msg(s1, 3, b"Hello!")
t, pl = recv_msg(s1)
assert t == 3, t
assert b"Client1" in pl and b"Hello!" in pl, pl
t, pl = recv_msg(s2)
assert t == 3, t
assert b"Client1" in pl and b"Hello!" in pl, pl

send_msg(s2, 4)
t, pl = recv_msg(s2)
assert t == 5, t

send_msg(s1, 6)
time.sleep(0.05)
s1.close()
s2.close()

print("ok")
PY
echo "tests passed"
