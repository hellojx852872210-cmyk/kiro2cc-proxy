#!/usr/bin/env python3
"""Network-isolated fake Kiro server. Never use real credentials here."""
import json
import ssl
import struct
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

LOCK = threading.Lock()
STATE = {"mode": "normal", "retry_after": "37", "hold_seconds": 2.0,
         "header_delay": 0.0, "calls": 0, "refresh_calls": 0, "active": 0,
         "inflight": 0, "max_active": 0, "mcp_calls": 0, "mcp_active": 0, "models": [], "hosts": [], "actors": [], "actor_calls": {}}


def frame(event, payload):
    headers = b""
    for name, value in [(":message-type", "event"), (":event-type", event),
                        (":content-type", "application/json")]:
        name, value = name.encode(), value.encode()
        headers += bytes([len(name)]) + name + b"\x07" + struct.pack(">H", len(value)) + value
    body = json.dumps(payload, separators=(",", ":")).encode()
    prelude = struct.pack(">II", 16 + len(headers) + len(body), len(headers))
    message = prelude + struct.pack(">I", zlib.crc32(prelude)) + headers + body
    return message + struct.pack(">I", zlib.crc32(message))


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass  # Never log authorization headers or request bodies.

    def reply(self, status, body, headers=None):
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(raw)
        self.wfile.flush()

    def rate_limit(self, retry_after, slow=False):
        if not slow:
            self.reply(429, {"message": "SERVICE_REQUEST_RATE_EXCEEDED"}, {"Retry-After": retry_after})
            return
        raw = b'{"message":"SERVICE_REQUEST_RATE_EXCEEDED"}'
        self.send_response(429)
        self.send_header("Content-Type", "application/json")
        self.send_header("Retry-After", retry_after)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.flush()
        time.sleep(2)  # The proxy must not wait for this error-body EOF.
        self.wfile.write(raw)
        self.wfile.flush()

    def do_GET(self):
        if self.path == "/state":
            with LOCK:
                snapshot = dict(STATE)
            self.reply(200, snapshot)
        else:
            self.reply(200, {"models": []})

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        try:
            obj = json.loads(raw or b"{}")
        except ValueError:
            self.reply(400, {"error": "invalid JSON"})
            return
        if self.path == "/configure":
            with LOCK:
                if STATE["inflight"]:
                    self.reply(409, {"error": "previous requests still active"})
                    return
                for key in ("mode", "retry_after", "hold_seconds", "header_delay"):
                    if key in obj:
                        STATE[key] = obj[key]
                STATE.update(calls=0, refresh_calls=0, active=0, max_active=0,
                             mcp_calls=0, mcp_active=0, models=[], hosts=[], actors=[], actor_calls={})
            self.reply(200, {"ok": True})
            return
        if "refresh" in self.path.lower() or self.path.rstrip("/").endswith("token"):
            with LOCK:
                STATE["refresh_calls"] += 1
                STATE["inflight"] += 1
                mode, retry_after = STATE["mode"], STATE["retry_after"]
            try:
                if mode == "refresh_hold":
                    time.sleep(2)
                if mode == "refresh429":
                    self.reply(429, {"message": "test refresh throttle"}, {"Retry-After": retry_after})
                else:
                    self.reply(200, {"accessToken": "offline-refreshed-token", "expiresIn": 3600,
                                     "refreshToken": "offline-refresh-token-" + "r" * 200})
            except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
                pass
            finally:
                with LOCK:
                    STATE["inflight"] -= 1
            return
        if self.path.rstrip("/") == "/mcp":
            with LOCK:
                STATE["mcp_calls"] += 1
                STATE["mcp_active"] += 1
                STATE["inflight"] += 1
                mode, retry_after = STATE["mode"], STATE["retry_after"]
            try:
                if mode in {"search_mcp429", "search_mcp429_hold"}:
                    self.rate_limit(retry_after, slow=mode.endswith("_hold"))
                    return
                result = {"id": obj.get("id", "test"), "jsonrpc": "2.0",
                          "result": {"isError": False, "content": [{"type": "text",
                                      "text": json.dumps({"results": [], "query": "offline"})}]}}
                if mode == "search_mcp_hold":
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    until = time.monotonic() + 10
                    while time.monotonic() < until:
                        self.chunk(b" ")
                        time.sleep(0.05)
                    self.chunk(json.dumps(result).encode())
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                else:
                    self.reply(200, result)
            except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
                pass
            finally:
                with LOCK:
                    STATE["mcp_active"] -= 1
                    STATE["inflight"] -= 1
            return
        if "generateAssistantResponse" not in self.path and "sendMessage" not in self.path:
            self.reply(200, {"models": [], "usageBreakdownList": []})
            return
        with LOCK:
            STATE["calls"] += 1
            STATE["inflight"] += 1
            call_number = STATE["calls"]
            mode, retry_after = STATE["mode"], STATE["retry_after"]
            hold, header_delay = STATE["hold_seconds"], STATE["header_delay"]
            user = obj.get("conversationState", {}).get("currentMessage", {}).get("userInputMessage", {})
            STATE["models"].append(user.get("modelId"))
            STATE["hosts"].append(self.headers.get("Host"))
            bearer = self.headers.get("Authorization", "")
            actor = bearer.rsplit("-", 1)[-1]
            actor = actor if actor.isdigit() else "1"
            STATE["actors"].append(actor)
            STATE["actor_calls"][actor] = STATE["actor_calls"].get(actor, 0) + 1
            actor_call = STATE["actor_calls"][actor]
        active = False
        try:
            if header_delay:
                time.sleep(header_delay)
            if mode == "rpm_plus_quota" and actor == "2":
                self.reply(402, {"reason": "MONTHLY_REQUEST_COUNT"})
                return
            if mode == "scope_quota" and actor == "1" and actor_call > 1:
                self.reply(402, {"reason": "MONTHLY_REQUEST_COUNT"})
                return
            first_soft_throttle = (mode == "mixed_hard_soft" and actor == "2" and actor_call == 1)
            first_soft_throttle |= (mode == "scope_quota" and actor == "1" and actor_call == 1)
            if first_soft_throttle or (mode == "first429_no_header" and call_number == 1):
                self.reply(429, {"message": "SERVICE_REQUEST_RATE_EXCEEDED"})
                return
            if mode in {"429", "429_hold"} or (mode == "search_continue429" and call_number > 1):
                self.rate_limit(retry_after, slow=mode == "429_hold")
                return
            with LOCK:
                STATE["active"] += 1
                STATE["max_active"] = max(STATE["max_active"], STATE["active"])
                active = True
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.amazon.eventstream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            if mode.startswith("search_") and call_number == 1:
                self.chunk(frame("toolUseEvent", {"name": "web_search", "toolUseId": "offline_search_1",
                                                 "input": "{\"query\":\"offline\"}", "stop": True}))
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
                return
            self.chunk(frame("assistantResponseEvent", {"content": "FIRST"}))
            if mode == "search_continue_disconnect" and call_number > 1:
                self.close_connection = True  # Missing final chunk: genuine HTTP body read error.
                return
            until = time.monotonic() + hold
            while time.monotonic() < until:
                time.sleep(0.05)
                self.chunk(frame("assistantResponseEvent", {"content": ""}))
            self.chunk(frame("assistantResponseEvent", {"content": " LAST"}))
            self.chunk(frame("meteringEvent", {"unit": "credit", "usage": 0.001}))
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass
        finally:
            with LOCK:
                STATE["inflight"] -= 1
                if active:
                    STATE["active"] -= 1

    def chunk(self, value):
        self.wfile.write(f"{len(value):x}\r\n".encode() + value + b"\r\n")
        self.wfile.flush()


if __name__ == "__main__":
    control = ThreadingHTTPServer(("0.0.0.0", 8080), Handler)
    threading.Thread(target=control.serve_forever, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", 443), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(Path("/fixtures/server.crt"), Path("/fixtures/server.key"))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()
