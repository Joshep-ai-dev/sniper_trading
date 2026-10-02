"""Isolated PAPER-only HTTP smoke test. No service credentials or signer are inherited."""
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def main():
    binary = Path(sys.argv[1] if len(sys.argv) > 1 else "target/debug/sniper-api").resolve()
    assert binary.is_file(), "build sniper-api before running the smoke test"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    token = secrets.token_hex(32)
    environment = {key: value for key, value in os.environ.items() if not key.startswith("SNIPER_")}
    with tempfile.TemporaryDirectory(prefix="sniper-paper-smoke-") as directory:
        root = Path(directory)
        configuration = root / "test.toml"
        configuration.write_text(
            f'mode = "PAPER"\nbind = "127.0.0.1:{port}"\n'
            f'browser_origin = "https://terminal.test"\nsecret_path = {json.dumps(str(root / "secrets"))}\n'
            '[store]\n'
            + "\n".join(f"{key} = {json.dumps(str(root / value))}" for key, value in [
                ("path", "db"), ("wal_dir", "wal"), ("checkpoint_path", "checkpoints"), ("backup_path", "backups")
            ])
            + '\nmin_free_bytes = 0\nwrite_buffer_bytes = 1048576\nblock_cache_bytes = 1048576\ncheckpoint_interval_secs = 0\n',
            encoding="utf-8",
        )
        environment.update(SNIPER_CONFIG=str(configuration), SNIPER_API_TOKEN=token, SNIPER_MASTER_KEY=secrets.token_hex(32))

        def call(path, body=None, authenticated=True, origin=None):
            headers = {"content-type": "application/json"}
            if authenticated:
                headers["authorization"] = f"Bearer {token}"
            if origin:
                headers["origin"] = origin
            request = Request(f"http://127.0.0.1:{port}{path}", data=None if body is None else json.dumps(body).encode(), headers=headers)
            try:
                with urlopen(request, timeout=15) as response:
                    return response.status, response.read()
            except HTTPError as error:
                return error.code, error.read()

        with (root / "backend.log").open("w", encoding="utf-8") as log:
            process = subprocess.Popen([str(binary)], env=environment, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 120
                while True:
                    assert process.poll() is None, "backend exited during smoke startup"
                    try:
                        status, body = call("/api/state")
                        if status == 200:
                            break
                    except URLError:
                        pass
                    assert time.monotonic() < deadline, "backend startup timed out"
                    time.sleep(0.1)
                state = json.loads(body)
                assert state["mode"] == "PAPER" and state["state"] == "PAUSED"
                assert state["balance"] == 5_000_000_000 and not state["positions"]
                assert call("/api/state", authenticated=False)[0] == 401
                assert call("/api/state", origin="https://attacker.test")[0] == 403
                assert call("/api/action", {"action": "START", "confirmation": None})[0] == 409
                assert call("/api/action", {"action": "SET_MODE", "mode": "LIVE"})[0] == 409
                assert json.loads(call("/api/state")[1])["mode"] == "PAPER"
                assert call("/api/credentials", {"field": "helius_rpc_url", "value": "https://invalid.test"})[0] == 409
                for endpoint in ["/api/history", "/api/analytics", "/api/analytics/buckets", "/api/credentials", "/api/wallet", "/metrics"]:
                    status, result = call(endpoint)
                    assert status == 200, endpoint
                    assert token.encode() not in result, "API token leaked in response"
                assert call("/api/database/checkpoint", {})[0] == 200
                assert call("/api/database/verify", {})[0] == 200
                assert call("/api/action", {"action": "STOP"})[0] == 200
                for _ in range(50):
                    if json.loads(call("/api/state")[1])["state"] == "STOPPED":
                        break
                    time.sleep(0.1)
                else:
                    raise AssertionError("STOP did not reach STOPPED with zero exposure")
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            assert process.returncode == 0, "backend did not shut down cleanly"
    print("PAPER smoke passed: authentication, Origin, mode and preflight guards, HTTPS credential guard, history, analytics, metrics, checkpoint, verify, STOP, clean shutdown")


if __name__ == "__main__":
    main()
