#!/usr/bin/env python3
"""Simple HTTP/1.0 test server for crawler testing"""
import socket, threading, sys

def handle(conn, addr):
    try:
        conn.recv(4096)  # read request
        body = b"Hello from test server\n"
        resp = (
            b"HTTP/1.0 200 OK\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Connection: close\r\n"
            b"\r\n" + body
        )
        conn.sendall(resp)
    except:
        pass
    finally:
        conn.close()

port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", port))
s.listen(128)
print(f"Test server listening on 0.0.0.0:{port}", flush=True)
while True:
    conn, addr = s.accept()
    threading.Thread(target=handle, args=(conn, addr), daemon=True).start()
