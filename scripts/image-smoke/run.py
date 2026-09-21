#!/usr/bin/env python3
"""Run a release image with fake credentials, TLS mock and probe on an internal network.
No Docker socket is mounted into any container. No real upstream can be reached.
"""
import argparse
import json
import pathlib
import secrets
import subprocess
import tempfile
import time
import uuid

HOSTS = ["q.us-east-1.amazonaws.com", "runtime.us-east-1.kiro.dev",
         "codewhisperer.us-east-1.amazonaws.com", "prod.us-east-1.auth.desktop.kiro.dev",
         "oidc.us-east-1.amazonaws.com"]


def cmd(*args, check=True):
    result = subprocess.run(args, text=True, capture_output=True)
    if check and result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {args[0:3]}: {result.stderr[-1000:]}")
    return result.stdout.strip()


class Harness:
    def __init__(self, image, root):
        self.image, self.root = image, root
        suffix = uuid.uuid4().hex[:10]
        self.network = "kiro-test-net-" + suffix
        self.mock, self.app, self.probe = ("kiro-test-" + role + "-" + suffix for role in ("mock", "app", "probe"))
        self.data, self.fixtures = root / "data", root / "fixtures"
        self.data.mkdir()
        self.fixtures.mkdir()
        self.directory = pathlib.Path(__file__).resolve().parent
        self.key = "offline-only-" + secrets.token_hex(8)
        self.created = []
        self.network_created = False

    def setup(self):
        if cmd("docker", "context", "show") != "colima":
            raise RuntimeError("refusing non-colima Docker context; this test must be local")
        self.image_id = cmd("docker", "image", "inspect", self.image, "--format", "{{.Id}}")
        cmd("docker", "image", "inspect", "python:3.12-alpine")  # Fail, never silently pull.
        f = self.fixtures
        cmd("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=Offline Kiro Test CA", "-keyout", str(f / "ca.key"), "-out", str(f / "ca.crt"),
            "-addext", "basicConstraints=critical,CA:TRUE")
        cmd("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=" + HOSTS[0],
            "-keyout", str(f / "server.key"), "-out", str(f / "server.csr"))
        (f / "extensions.cnf").write_text("subjectAltName=" + ",".join("DNS:" + h for h in HOSTS) +
                                           "\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n")
        cmd("openssl", "x509", "-req", "-in", str(f / "server.csr"), "-CA", str(f / "ca.crt"),
            "-CAkey", str(f / "ca.key"), "-CAcreateserial", "-days", "1", "-out", str(f / "server.crt"),
            "-extfile", str(f / "extensions.cnf"))
        cmd("docker", "network", "create", "--internal", self.network)
        self.network_created = True
        aliases = [item for host in HOSTS for item in ("--network-alias", host)]
        self.start_container(self.mock, "--network", self.network,
                             *aliases, "-v", str(f) + ":/fixtures:ro", "-v",
                             str(self.directory / "mock_upstream.py") + ":/mock.py:ro",
                             "python:3.12-alpine", "python", "/mock.py")
        self.start_container(self.probe, "--network", self.network,
                             "-v", str(self.directory) + ":/runner:ro", "-v", str(self.data) + ":/data:ro",
                             "-e", "APP_HOST=" + self.app, "-e", "MOCK_HOST=" + self.mock,
                             "python:3.12-alpine", "python", "-c", "import time; time.sleep(7200)")
        self.execute("wait", "mock")
        assert json.loads(cmd("docker", "network", "inspect", self.network))[0]["Internal"] is True

    def start_container(self, name, *args):
        cmd("docker", "create", "--pull=never", "--name", name, *args)
        self.created.append(name)  # Register before start, so startup failures are cleaned too.
        cmd("docker", "start", name)

    def execute(self, action, *args):
        result = cmd("docker", "exec", self.probe, "python", "/runner/probe.py", action, *args)
        return json.loads(result)

    def stop_app(self):
        if self.app in self.created:
            cmd("docker", "rm", "-f", self.app)
            self.created.remove(self.app)
            self.execute("drain")

    def start(self, *, mode="normal", rpm=0, global_limit=10, account_limit=10,
              hold=0.1, header_delay=0, expired=False, retry_after="37"):
        self.stop_app()
        for path in self.data.iterdir():
            if path.is_file():
                path.unlink()  # Only this unique TemporaryDirectory, dummy fixtures only.
        config = {"host": "0.0.0.0", "port": 5678, "region": "us-east-1", "tlsBackend": "rustls",
                  "adminPsw": "offline-admin", "loadBalancingMode": "balanced",
                  "maxRpmPerCredential": rpm, "maxConcurrentRequests": global_limit,
                  "maxConcurrentPerCredential": account_limit, "admissionTimeoutMs": 400,
                  "maxAdmissionWaiters": 64, "cacheCreationSplitRatio": 0.1768}
        credentials = [{"id": 1, "accessToken": "offline-access-token", "refreshToken": "x" * 200,
                        "expiresAt": "2020-01-01T00:00:00Z" if expired else "2099-01-01T00:00:00Z",
                        "authMethod": "social", "subscriptionTitle": "KIRO PRO+", "priority": 0,
                        "profileArn": "arn:aws:codewhisperer:us-east-1:000000000000:profile/offline"}]
        keys = [{"id": 1, "key": self.key, "name": "offline-test", "enabled": True,
                 "createdAt": "2026-01-01T00:00:00Z", "boundCredentialIds": [1]}]
        for name, value in (("config.json", config), ("credentials.json", credentials), ("api_keys.json", keys)):
            (self.data / name).write_text(json.dumps(value))
        self.execute("configure", json.dumps({"mode": mode, "retry_after": retry_after,
                                             "hold_seconds": hold, "header_delay": header_delay}))
        self.start_container(self.app, "--platform", "linux/amd64",
            "--network", self.network, "-v", str(self.data) + ":/app/config",
            "-v", str(self.fixtures / "ca.crt") + ":/usr/local/share/ca-certificates/offline-test.crt:ro",
            "--entrypoint", "sh", self.image_id, "-c", "update-ca-certificates >/dev/null 2>&1; "
            "exec /app/kiro2cc-proxy --config /app/config/config.json --credentials /app/config/credentials.json")
        self.execute("wait", "app")

    def close(self):
        failures = []
        for name in reversed(self.created):
            try:
                cmd("docker", "rm", "-f", name)
            except RuntimeError as exc:
                failures.append(str(exc))
        if self.network_created:
            try:
                cmd("docker", "network", "rm", self.network)
            except RuntimeError as exc:
                failures.append(str(exc))
        if failures:
            raise RuntimeError("cleanup failed: " + "; ".join(failures))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    results = []
    # Colima shares the user workspace, not macOS /var/folders TemporaryDirectory.
    output_dir = pathlib.Path(args.out).resolve().parent
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="kiro-image-smoke-", dir=output_dir) as directory:
        h = Harness(args.image, pathlib.Path(directory))
        try:
            h.setup()
            cases = [("auth_alias_cache", {}, ["smoke"])]
            routes = ["/v1/messages", "/cc/v1/messages", "/v1/chat/completions", "/v1/responses"]
            cases += [("retry_after_" + route, {"mode": "429"}, ["rate_limit", route]) for route in routes]
            cases += [("realtime_" + route, {"hold": 0.8}, ["stream", route]) for route in routes]
            cases += [
                ("atomic_rpm_16_limit_8", {"rpm": 8, "global_limit": 32, "account_limit": 32,
                                          "header_delay": 0.15}, ["rpm"]),
                ("existing_account_limit_20", {"global_limit": 50, "account_limit": 20, "hold": 10}, ["capacity"]),
                ("global_lease_cancel", {"global_limit": 1, "account_limit": 10, "hold": 10}, ["lease"]),
                ("account_lease_cancel", {"global_limit": 10, "account_limit": 1, "hold": 10}, ["lease"]),
                ("refresh_429_recovery", {"mode": "refresh429", "expired": True, "retry_after": "2"}, ["refresh"]),
            ]
            for name, config, action in cases:
                started = time.monotonic()
                try:
                    h.start(**config)
                    detail = h.execute(*action)
                    result = {"name": name, "ok": True, "detail": detail}
                except Exception as exc:
                    result = {"name": name, "ok": False, "error": str(exc)}
                result["seconds"] = round(time.monotonic() - started, 3)
                results.append(result)
                print(json.dumps(result, ensure_ascii=False), flush=True)
        finally:
            h.close()
    pathlib.Path(args.out).write_text(json.dumps({"image": args.image, "image_id": h.image_id,
                                                 "network_internal": True, "tests": results}, indent=2))
    return 0 if results and all(item["ok"] for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
