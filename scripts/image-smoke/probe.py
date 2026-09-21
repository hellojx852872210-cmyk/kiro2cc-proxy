#!/usr/bin/env python3
"""Runs inside the isolated probe container. No Docker socket, no production keys."""
import concurrent.futures
import http.client
import json
import os
import pathlib
import sys
import threading
import time

APP, MOCK = os.environ["APP_HOST"], os.environ["MOCK_HOST"]


def key():
    return json.loads(pathlib.Path("/data/api_keys.json").read_text())[0]["key"]


def request(route="/v1/messages", stream=False, model="claude-sonnet-4-5"):
    if "responses" in route:
        return {"model": model, "input": "Say FIRST LAST", "max_output_tokens": 32, "stream": stream}
    value = {"model": model, "messages": [{"role": "user", "content": "Say FIRST LAST"}],
             "max_tokens": 32, "stream": stream}
    if "messages" in route:
        value["system"] = "Offline integration test. " * 100
    return value


def call(host, port, path, payload=None, authenticated=False, timeout=15):
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    headers = {"Content-Type": "application/json"}
    if authenticated:
        headers["x-api-key"] = key()
    try:
        conn.request("POST" if payload is not None else "GET", path,
                     json.dumps(payload) if payload is not None else None, headers)
        response = conn.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        conn.close()


def api(route="/v1/messages", stream=False, model="claude-sonnet-4-5"):
    return call(APP, 5678, route, request(route, stream, model), True)


def state():
    return json.loads(call(MOCK, 8080, "/state")[2])


def configure(values):
    code, _, raw = call(MOCK, 8080, "/configure", values)
    assert code == 200, raw
    return json.loads(raw)


def search_request(route, stream=False, pure=False):
    payload = request(route, stream)
    schema = {"type": "object", "properties": {"query": {"type": "string"}}}
    functions = [{"name": "web_search", "description": "Search offline fixtures", "parameters": schema}]
    if not pure:
        functions.append({"name": "noop", "description": "Unused", "parameters": {"type": "object"}})
    if "chat/completions" in route:
        payload["tools"] = [{"type": "function", "function": f} for f in functions]
    elif "responses" in route:
        payload["tools"] = [{"type": "function", **f} for f in functions]
    else:
        payload["tools"] = [{"name": f["name"], "description": f["description"],
                              "input_schema": f["parameters"]} for f in functions]
        payload["tools"][0].update(type="web_search_20250305", max_uses=1)
    return payload


def events(response, allow_errors=False):
    data = []
    while True:
        line = response.readline()
        if not line:
            assert not data, "truncated SSE frame"
            return
        line = line.decode("utf-8").rstrip("\r\n")
        if not line:
            if data:
                joined = "\n".join(data)
                data = []
                if joined == "[DONE]":
                    yield {"type": "_done"}
                else:
                    item = json.loads(joined)
                    if not allow_errors:
                        assert item.get("type") != "error" and "error" not in item, item
                    yield item
        elif line.startswith("data:"):
            data.append(line[5:].lstrip())


def content(item, route):
    if "chat/completions" in route:
        return "".join(choice.get("delta", {}).get("content") or "" for choice in item.get("choices", []))
    if "responses" in route:
        return item.get("delta", "") if item.get("type") == "response.output_text.delta" else ""
    return item.get("delta", {}).get("text", "") if item.get("type") == "content_block_delta" else ""


def open_stream(route):
    conn = http.client.HTTPConnection(APP, 5678, timeout=15)
    conn.request("POST", route, json.dumps(request(route, True)),
                 {"x-api-key": key(), "Content-Type": "application/json"})
    response = conn.getresponse()
    assert response.status == 200, (response.status, response.read()[:150])
    return conn, response


def run(action, args):
    if action == "wait":
        host, port, path = (MOCK, 8080, "/state") if args[0] == "mock" else (APP, 5678, "/admin/")
        until = time.monotonic() + 30
        while time.monotonic() < until:
            try:
                call(host, port, path, timeout=1)
                return {"ready": True}
            except (OSError, http.client.HTTPException):
                time.sleep(0.1)
        raise AssertionError("container did not become ready")
    if action == "configure":
        return configure(json.loads(args[0]))
    if action == "state":
        return state()
    if action == "drain":
        until = time.monotonic() + 5
        while time.monotonic() < until:
            current = state()
            if current.get("inflight", 0) == 0:
                return {"drained": True}
            time.sleep(0.1)
        raise AssertionError("old mock requests did not drain")
    if action == "smoke":
        assert call(APP, 5678, "/v1/messages", request())[0] == 401
        status, _, raw = api(model="claude-fable-5.1")
        assert status == 200, (status, raw[:200])
        answer = json.loads(raw)
        assert "FIRST" in json.dumps(answer)
        assert state()["models"][-1] == "claude-opus-5", state()
        assert answer["usage"]["cache_creation_input_tokens"] > 0, answer["usage"]
        return {"alias": "opus-5", "cache_split_preserved": True}
    if action == "rate_limit":
        status, headers, raw = api(args[0], True)
        assert status == 429, (status, raw[:160])
        assert headers.get("retry-after") == "37", headers
        assert state()["calls"] == 1, state()
        return {"http": 429, "retry_after": "37", "attempts": 1}
    if action == "stream":
        route = args[0]
        conn, response = open_stream(route)
        first = last = None
        terminal, chat_finish, output = False, False, ""
        try:
            for item in events(response):
                text = content(item, route)
                if text:
                    output += text
                    if first is None:
                        first = time.monotonic()
                    last = time.monotonic()
                if "chat/completions" in route:
                    chat_finish |= any(c.get("finish_reason") == "stop" for c in item.get("choices", []))
                    terminal |= item.get("type") == "_done"
                elif "responses" in route:
                    assert item.get("type") not in {"response.failed", "response.incomplete"}, item
                    if item.get("type") == "response.completed":
                        assert item.get("response", {}).get("status") == "completed", item
                        terminal = True
                else:
                    terminal |= item.get("type") == "message_stop"
        finally:
            response.close()
            conn.close()
        assert "FIRST" in output and "LAST" in output and terminal, (output, terminal)
        if "chat/completions" in route:
            assert chat_finish, "missing successful Chat finish_reason"
        assert last - first >= 0.3, f"buffered rather than incremental: {last - first}"
        return {"complete": True, "first_to_last_seconds": round(last - first, 3)}
    if action == "rpm":
        barrier = threading.Barrier(16)
        def one(_):
            barrier.wait()
            return api()[0]
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            statuses = list(pool.map(one, range(16)))
        assert state()["calls"] == 8, {"state": state(), "statuses": statuses}
        assert statuses.count(200) == 8 and statuses.count(429) == 8, statuses
        return {"success": 8, "limited": 8, "upstream_calls": 8}
    if action == "capacity":
        # 20 is also the old binary's compiled per-account default: this is not
        # merely a failure caused by an old binary ignoring new config fields.
        barrier = threading.Barrier(21)
        opened = []
        started = time.monotonic()
        def held(_):
            connection = http.client.HTTPConnection(APP, 5678, timeout=8)
            barrier.wait()
            connection.request("POST", "/v1/messages", json.dumps(request(stream=True)),
                               {"x-api-key": key(), "Content-Type": "application/json"})
            response = connection.getresponse()
            opened.append((connection, response))
            if response.status == 200:
                assert any("FIRST" in content(item, "/v1/messages") for item in events(response))
            else:
                response.read()
            return response.status
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=21) as pool:
                statuses = list(pool.map(held, range(21)))
            snapshot = state()
            assert time.monotonic() - started < 5, "test outlived the overlap window"
            assert statuses.count(200) == 20 and statuses.count(429) == 1, {
                "statuses": statuses, "max_active": snapshot["max_active"]}
            assert snapshot["max_active"] <= 20, snapshot
            return {"admitted": 20, "limited": 1, "max_active": snapshot["max_active"]}
        finally:
            for connection, response in opened:
                response.close()
                connection.close()
    if action == "lease":
        route = "/cc/v1/messages"
        conn, response = open_stream(route)
        try:
            assert any("FIRST" in content(item, route) for item in events(response))
            first_seen = time.monotonic()
            status, _, raw = api()
            assert status == 429, f"permit released at headers: {status} {raw[:160]!r}"
            assert time.monotonic() - first_seen < 2, "busy request waited instead of bounded admission"
            current = state()
            assert current["calls"] == 1 and current["active"] == 1, current
            assert time.monotonic() - first_seen < 3, "first stream may already have reached natural EOF"
        finally:
            response.close()
            conn.close()
        cancelled = time.monotonic()
        time.sleep(0.2)
        next_conn, next_response = open_stream(route)
        try:
            assert any("FIRST" in content(item, route) for item in events(next_response))
            delay = time.monotonic() - cancelled
            assert delay < 2, f"cancellation waited for original 10s EOF: {delay}"
            assert state()["max_active"] <= 1, state()
        finally:
            next_response.close()
            next_conn.close()
        return {"cancel_to_next_content_seconds": round(delay, 3), "max_active": 1}
    if action == "search_http":
        route, expected = args[0], int(args[1])
        pure = len(args) > 2 and args[2] == "pure"
        status, headers, raw = call(APP, 5678, route, search_request(route, pure=pure), True)
        assert status == expected, (status, raw[:200], state())
        if expected == 429:
            assert headers.get("retry-after") == "37", headers
        current = state()
        assert current["mcp_calls"] == 1, current
        expected_models = 0 if pure else (1 if current["mode"] == "search_mcp429" else 2)
        assert current["calls"] == expected_models, current
        if expected == 200:
            assert "LAST" in json.dumps(json.loads(raw)), raw[:200]
        return {"http": status, "model_calls": current["calls"], "mcp_calls": 1}
    if action in {"search_stream_error", "search_cancel"}:
        conn = http.client.HTTPConnection(APP, 5678, timeout=8)
        conn.request("POST", "/v1/messages", json.dumps(search_request("/v1/messages", stream=True)),
                     {"x-api-key": key(), "Content-Type": "application/json"})
        response = conn.getresponse()
        try:
            assert response.status == 200
            if action == "search_stream_error":
                received = list(events(response, allow_errors=True))
                assert any(item.get("type") == "error" for item in received), received
                assert not any(item.get("type") == "message_stop" for item in received), received
                assert state()["calls"] == 1 and state()["mcp_calls"] == 1, state()
                return {"error_event": True, "continuation_calls": 0}
            until = time.monotonic() + 3
            while time.monotonic() < until and state()["mcp_active"] == 0:
                time.sleep(0.05)
            assert state()["mcp_active"] == 1, state()
        finally:
            response.close()
            conn.close()
        cancelled = time.monotonic()
        while time.monotonic() - cancelled < 2 and state()["mcp_active"]:
            time.sleep(0.05)
        current = state()
        assert current["mcp_active"] == 0 and current["calls"] == 1, current
        configure({"mode": "normal", "hold_seconds": 0.05})
        assert api()[0] == 200, "global/account slot not restored after MCP cancellation"
        return {"mcp_cancel_seconds": round(time.monotonic() - cancelled, 3), "continuation_calls": 0}
    if action == "refresh_deadline":
        started = time.monotonic()
        status, headers, raw = api()
        elapsed = time.monotonic() - started
        assert status == 429 and "retry-after" in headers, (status, raw[:150])
        assert elapsed < 1.5, f"400ms admission waited for 2s refresh: {elapsed}"
        assert state()["calls"] == 0 and state()["refresh_calls"] == 1, state()
        return {"seconds": round(elapsed, 3), "model_calls": 0}
    if action == "cooldown_all":
        results = [api() for _ in range(5)]
        assert [item[0] for item in results] == [429] * 5, [item[0] for item in results]
        assert all(1 <= int(item[1].get("retry-after", "0")) <= 37 for item in results)
        assert state()["calls"] == 4, state()
        return {"client_429": 5, "upstream_calls": 4, "all_buckets_cold_still_429": True}
    if action == "refresh":
        responses = [api() for _ in range(3)]
        assert [r[0] for r in responses] == [429] * 3, [r[0] for r in responses]
        assert all("retry-after" in r[1] for r in responses)
        assert state()["refresh_calls"] == 1 and state()["calls"] == 0, state()
        time.sleep(2.2)
        configure({"mode": "normal", "hold_seconds": 0.05})
        status, _, raw = api()
        assert status == 200, (status, raw[:200])
        assert state()["refresh_calls"] == 1 and state()["calls"] == 1, state()
        return {"limited_refresh_calls": 1, "recovered_refresh_calls": 1, "recovered": True}
    raise AssertionError("unknown action")


if __name__ == "__main__":
    print(json.dumps(run(sys.argv[1], sys.argv[2:]), ensure_ascii=False))
