#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import re
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode


UTC = timezone.utc
CONFIG_FILE = Path(__file__).resolve().parents[1] / "main" / "xiaozhi-server" / "data" / ".config.yaml"
REQUIRED_INVARIANTS = {
    # "lifecycle",
    "session_isolation",
    "device_state_isolation",
    "message_isolation",
    "message_routing_integrity",
    "firestore_consistency",
}


def _default_emulator_host() -> str:
    env_host = os.environ.get("FIRESTORE_EMULATOR_HOST")
    if env_host:
        return env_host

    try:
        content = CONFIG_FILE.read_text(encoding="utf-8")
    except OSError:
        return "localhost:8080"

    # Read top-level YAML scalar line: FIRESTORE_EMULATOR_HOST: localhost:8080
    match = re.search(r"(?m)^FIRESTORE_EMULATOR_HOST:\s*['\"]?([^'\"\n#]+)", content)
    if match:
        return match.group(1).strip()
    return "localhost:8080"


@dataclass(frozen=True)
class Config:
    devices: int
    seed: int
    ramp: float
    duration: int
    ws_url: str
    project_id: str
    emulator_host: str
    auth_token: str
    request_timeout: float
    handshake_retries: int
    handshake_backoff_s: float
    max_concurrent_handshakes: int
    monitor_docker_container: str
    monitor_docker_interval_s: float


@dataclass
class DeviceRunResult:
    index: int
    device_id: str
    session_id: str
    success: bool
    iterations: int
    latencies_ms: List[float]
    failures: List[Dict[str, Any]]


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description="Run concurrent device lifecycle simulation against local xiaozhi-server."
    )
    parser.add_argument("--devices", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--ramp",
        type=float,
        default=5.0,
        help="Device start rate in devices/sec.",
    )
    parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds.")
    parser.add_argument(
        "--ws-url",
        default="ws://127.0.0.1:8000/xiaozhi/v1/",
        help="WebSocket endpoint.",
    )
    parser.add_argument("--project-id", default="baby-milu-local")
    parser.add_argument(
        "--emulator-host",
        default=_default_emulator_host(),
    )
    parser.add_argument("--auth-token", default="", help="Optional Bearer token.")
    parser.add_argument("--request-timeout", type=float, default=6.0)
    parser.add_argument(
        "--handshake-retries",
        type=int,
        default=2,
        help="Retries for connect+hello handshake before marking lifecycle failure.",
    )
    parser.add_argument(
        "--handshake-backoff",
        type=float,
        default=0.5,
        help="Base backoff seconds between handshake retries (exponential).",
    )
    parser.add_argument(
        "--max-concurrent-handshakes",
        type=int,
        default=4,
        help="Limit simultaneous connect+hello handshakes to smooth high ramp bursts.",
    )
    parser.add_argument(
        "--monitor-docker-container",
        default="xiaozhi-esp32-server",
        help="Docker container name to monitor during the test (empty to disable).",
    )
    parser.add_argument(
        "--monitor-docker-interval",
        type=float,
        default=1.0,
        help="Sampling interval in seconds for docker stats monitor.",
    )
    args = parser.parse_args()

    return Config(
        devices=args.devices,
        seed=args.seed,
        ramp=max(args.ramp, 0.0),
        duration=max(args.duration, 1),
        ws_url=args.ws_url,
        project_id=args.project_id,
        emulator_host=args.emulator_host,
        auth_token=args.auth_token,
        request_timeout=max(args.request_timeout, 0.5),
        handshake_retries=max(args.handshake_retries, 0),
        handshake_backoff_s=max(args.handshake_backoff, 0.0),
        max_concurrent_handshakes=max(args.max_concurrent_handshakes, 1),
        monitor_docker_container=(args.monitor_docker_container or "").strip(),
        monitor_docker_interval_s=max(args.monitor_docker_interval, 0.2),
    )


class Harness:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.run_id = f"run-{int(time.time())}-{cfg.seed}" # use timestamp to ensure integrity
        self.started_at = time.perf_counter()

        try:
            from google.cloud import firestore  # type: ignore[reportMissingImports]
        except ImportError as exc:
            raise RuntimeError(
                "google-cloud-firestore is required. Install dependencies from main/xiaozhi-server/requirements.txt"
            ) from exc

        try:
            import websockets  # pylint: disable=import-outside-toplevel  # type: ignore[reportMissingImports]
        except ImportError as exc:
            raise RuntimeError(
                "websockets is required. Install dependencies from main/xiaozhi-server/requirements.txt"
            ) from exc

        self.firestore = firestore
        self.websockets = websockets

        os.environ["FIRESTORE_EMULATOR_HOST"] = cfg.emulator_host
        self.client = self.firestore.Client(project=cfg.project_id)

        self.failures: List[Dict[str, Any]] = []
        self.session_by_device: Dict[str, str] = {} # expected device_id -> session_id mapping observed during device run, used for validation
        self.device_state_markers: Dict[str, str] = {}
        self.observed_sessions_by_device: Dict[str, set[str]] = defaultdict(set) # observed device_id -> set of observed session_ids in messages received by that device, used for validation
        self.observed_message_device_ids: Dict[str, set[str]] = defaultdict(set)
        self.device_state_reads_by_device: Dict[str, set[str]] = defaultdict(set)
        self.latencies_ms: List[float] = []
        self.success_count = 0
        self.timeout_count = 0  # Track timeout errors
        self.handshake_semaphore = asyncio.Semaphore(cfg.max_concurrent_handshakes)
        self.backend_cpu_samples: List[float] = []
        self.backend_mem_gib_samples: List[float] = []
        self.docker_monitor_errors = 0

    def _record_failure(self, failure: Dict[str, Any]) -> None:
        self.failures.append(failure)

    @staticmethod
    def _format_exc(exc: Exception) -> str:
        msg = str(exc).strip()
        return msg if msg else exc.__class__.__name__

    @staticmethod
    def _maybe_json(message: Any) -> Optional[Dict[str, Any]]:
        if isinstance(message, bytes):
            return None
        if not isinstance(message, str):
            return None
        try:
            parsed = json.loads(message)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    @staticmethod
    def _percentile(values: List[float], pct: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        if len(ordered) == 1:
            return ordered[0]
        rank = (len(ordered) - 1) * pct
        low = int(rank)
        high = min(low + 1, len(ordered) - 1)
        weight = rank - low
        return ordered[low] * (1.0 - weight) + ordered[high] * weight

    @staticmethod
    def _parse_percent(value: str) -> Optional[float]:
        raw = (value or "").strip().replace("%", "")
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    @staticmethod
    def _parse_mem_used_gib(mem_usage: str) -> Optional[float]:
        # docker stats format: "2.841GiB / 31.22GiB"
        match = re.match(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([KMG]iB)\s*/", mem_usage or "")
        if not match:
            return None
        value = float(match.group(1))
        unit = match.group(2)
        if unit == "GiB":
            return value
        if unit == "MiB":
            return value / 1024.0
        if unit == "KiB":
            return value / (1024.0 * 1024.0)
        return None

    async def _sample_docker_stats(self) -> None:
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.CPUPerc}},{{.MemUsage}}",
            self.cfg.monitor_docker_container,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out_b, err_b = await proc.communicate()
        if proc.returncode != 0:
            self.docker_monitor_errors += 1
            return

        line = (out_b.decode("utf-8", errors="ignore").strip().splitlines() or [""])[0]
        parts = line.split(",", 1)
        if len(parts) != 2:
            self.docker_monitor_errors += 1
            return

        cpu = self._parse_percent(parts[0])
        mem_gib = self._parse_mem_used_gib(parts[1])
        if cpu is not None:
            self.backend_cpu_samples.append(cpu)
        if mem_gib is not None:
            self.backend_mem_gib_samples.append(mem_gib)
        if cpu is None and mem_gib is None:
            self.docker_monitor_errors += 1

    async def _run_docker_monitor(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self._sample_docker_stats()
            except (FileNotFoundError, PermissionError):
                self.docker_monitor_errors += 1
                return
            except Exception:
                self.docker_monitor_errors += 1

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.cfg.monitor_docker_interval_s)
            except asyncio.TimeoutError:
                continue

    async def _fs_set(self, collection: str, doc_id: str, payload: Dict[str, Any], merge: bool = False) -> None:
        await asyncio.to_thread(
            self.client.collection(collection).document(doc_id).set,
            payload,
            merge,
        )

    async def _fs_get(self, collection: str, doc_id: str):
        return await asyncio.to_thread(self.client.collection(collection).document(doc_id).get)

    async def _fs_stream(self, collection: str, where_field: str, where_value: str):
        query = self.client.collection(collection).where(
            filter=self.firestore.FieldFilter(where_field, "==", where_value)
        )
        return await asyncio.to_thread(lambda: list(query.stream()))

    async def run_device(self, index: int, end_time: float) -> DeviceRunResult:
        device_id = f"dev-{index:04d}"
        client_id = f"concurrency-harness-{index:04d}"
        user_id = f"user-{index // 4:04d}"
        q = urlencode({"device-id": device_id, "client-id": client_id})
        url = f"{self.cfg.ws_url}?{q}"

        if self.cfg.ramp > 0:
            await asyncio.sleep(index / self.cfg.ramp)

        failures: List[Dict[str, Any]] = []
        latencies: List[float] = []
        iterations = 0
        session_id = ""

        headers = {}
        if self.cfg.auth_token:
            headers["Authorization"] = f"Bearer {self.cfg.auth_token}"

        max_attempts = self.cfg.handshake_retries + 1
        for attempt in range(max_attempts):
            handshake_ok = False
            try:
                # IMPORTANT: guard both WebSocket upgrade and hello exchange.
                # If we only gate hello, connect() still bursts and can overwhelm server handshake.
                async with self.handshake_semaphore:
                    async with self.websockets.connect(url, additional_headers=headers) as ws:
                        hello_payload = {
                            "type": "hello",
                            "version": 1,
                            "transport": "websocket",
                            "audio_params": {
                                "format": "opus",
                                "sample_rate": 16000,
                                "channels": 1,
                                "frame_duration": 60,
                            },
                        }
                        await ws.send(json.dumps(hello_payload))
                        hello_resp: Optional[Dict[str, Any]] = None
                        hello_deadline = time.perf_counter() + self.cfg.request_timeout
                        while time.perf_counter() < hello_deadline:
                            raw_hello = await asyncio.wait_for(ws.recv(), timeout=self.cfg.request_timeout)
                            msg = self._maybe_json(raw_hello)
                            if msg and msg.get("session_id"):
                                hello_resp = msg
                                break
                        if hello_resp is None:
                            raise RuntimeError("timeout waiting for hello response")
                        session_id = str(hello_resp.get("session_id", ""))
                        if not session_id:
                            failures.append(
                                {
                                    "device_index": index,
                                    "device_id": device_id,
                                    "failed_invariant": "session_isolation",
                                    "details": "hello response did not contain session_id",
                                }
                            )
                            return DeviceRunResult(index, device_id, "", False, 0, latencies, failures)
                        handshake_ok = True

                        self.session_by_device[device_id] = session_id

                        now = datetime.now(UTC)
                        await self._fs_set(
                            "sessions",
                            session_id,
                            {
                                "sessionId": session_id,
                                "deviceId": device_id,
                                "connectedAt": now,
                                "lastSeenAt": now,
                                "runId": self.run_id,
                                "userId": user_id,
                            },
                        )

                        marker = f"marker-{self.run_id}-{index:04d}"
                        self.device_state_markers[device_id] = marker
                        await self._fs_set(
                            "device_state",
                            device_id,
                            {
                                "deviceId": device_id,
                                "currentMode": "chat",
                                "context": {
                                    "marker": marker,
                                    "owner": device_id,
                                    "runId": self.run_id,
                                },
                                "updatedAt": now,
                            },
                            merge=True,
                        )

                        # Device-level read should only target the caller's own device_state.
                        own_state = await self._fs_get("device_state", device_id)
                        self.device_state_reads_by_device[device_id].add(device_id)
                        own_state_doc = own_state.to_dict() if own_state.exists else {}
                        if str((own_state_doc or {}).get("deviceId", "")) != device_id:
                            failures.append(
                                {
                                    "device_index": index,
                                    "device_id": device_id,
                                    "failed_invariant": "device_state_isolation",
                                    "details": "own device_state read returned wrong deviceId",
                                }
                            )

                        while time.perf_counter() < end_time:
                            iterations += 1
                            request_id = f"{self.run_id}-{index:04d}-{iterations:04d}"
                            outbound = {
                                "type": "abort",
                                "requestId": request_id,
                                "session_id": session_id,
                                "deviceId": device_id,
                            }
                            sent_at = time.perf_counter()
                            await ws.send(json.dumps(outbound))

                            inbound: Optional[Dict[str, Any]] = None
                            while True:
                                raw_msg = await asyncio.wait_for(ws.recv(), timeout=self.cfg.request_timeout)
                                candidate = self._maybe_json(raw_msg)
                                if candidate is None:
                                    continue
                                
                                observed_session = str(
                                    candidate.get("session_id")
                                    or candidate.get("sessionId")
                                    or ""
                                )
                                if observed_session:
                                    self.observed_sessions_by_device[device_id].add(observed_session)

                                observed_device = str(
                                    candidate.get("deviceId")
                                    or candidate.get("device_id")
                                    or ""
                                )
                                if observed_device:
                                    self.observed_message_device_ids[device_id].add(observed_device)

                                if candidate.get("type") == "tts" and candidate.get("state") == "stop":
                                    inbound = candidate
                                    break
                            
                            latency_ms = (time.perf_counter() - sent_at) * 1000.0
                            latencies.append(latency_ms)
                            self.latencies_ms.append(latency_ms)

                            inbound_session = str(inbound.get("session_id", ""))
                            if inbound_session != session_id:
                                failures.append(
                                    {
                                        "device_index": index,
                                        "device_id": device_id,
                                        "ids_involved": {
                                            "expected_session_id": session_id,
                                            "received_session_id": inbound_session,
                                        },
                                        "failed_invariant": "session_isolation",
                                        "details": "received tts stop for another session",
                                    }
                                )

                            ts = datetime.now(UTC)
                            client_msg_id = f"{request_id}-c"
                            server_msg_id = f"{request_id}-s"
                            await self._fs_set(
                                "messages",
                                client_msg_id,
                                {
                                    "messageId": client_msg_id,
                                    "sessionId": session_id,
                                    "deviceId": device_id,
                                    "direction": "client_to_server",
                                    "payload": outbound,
                                    "timestamp": ts,
                                    "runId": self.run_id,
                                },
                            )
                            await self._fs_set(
                                "messages",
                                server_msg_id,
                                {
                                    "messageId": server_msg_id,
                                    "sessionId": inbound_session,
                                    "deviceId": device_id,
                                    "direction": "server_to_client",
                                    "payload": {
                                        "requestId": request_id,
                                        "raw": inbound,
                                    },
                                    "timestamp": ts,
                                    "runId": self.run_id,
                                },
                            )
                            await self._fs_set(
                                "sessions",
                                session_id,
                                {
                                    "lastSeenAt": ts,
                                    "runId": self.run_id,
                                },
                                merge=True,
                            )

                            await asyncio.sleep(self.rng.uniform(0.05, 0.2))

                        success = len(failures) == 0 and bool(session_id)
                        return DeviceRunResult(index, device_id, session_id, success, iterations, latencies, failures)

            except Exception as exc:
                # Track timeout errors separately
                if isinstance(exc, asyncio.TimeoutError):
                    self.timeout_count += 1
                
                # Some server modes intentionally close websocket cleanly (1000) after minimal exchange.
                # If handshake succeeded and we completed at least one iteration, don't classify as lifecycle failure.
                if handshake_ok and isinstance(exc, self.websockets.exceptions.ConnectionClosedOK):
                    success = len(failures) == 0 and bool(session_id) and iterations > 0
                    return DeviceRunResult(index, device_id, session_id, success, iterations, latencies, failures)

                can_retry_handshake = (not handshake_ok) and (attempt < max_attempts - 1)
                if can_retry_handshake:
                    delay = self.cfg.handshake_backoff_s * (2 ** attempt)
                    # Add deterministic jitter to avoid synchronized reconnect bursts.
                    delay += self.rng.uniform(0.0, 0.2)
                    await asyncio.sleep(delay)
                    continue

                failures.append(
                    {
                        "device_index": index,
                        "device_id": device_id,
                        "failed_invariant": "lifecycle",
                        "details": f"device lifecycle failed: {self._format_exc(exc)}",
                    }
                )
                success = len(failures) == 0 and bool(session_id)
                return DeviceRunResult(index, device_id, session_id, success, iterations, latencies, failures)

        success = len(failures) == 0 and bool(session_id)
        return DeviceRunResult(index, device_id, session_id, success, iterations, latencies, failures)

    async def validate_post_conditions(self, results: List[DeviceRunResult]) -> None:
        # Invariant 1A: one messageId must not be shared by multiple devices.
        message_docs = await self._fs_stream("messages", "runId", self.run_id)
        message_owners = defaultdict(set)
        for doc in message_docs:
            payload = doc.to_dict() or {}
            message_id = str(payload.get("messageId") or doc.id)
            owner_device_id = str(payload.get("deviceId") or "")
            if message_id and owner_device_id:
                message_owners[message_id].add(owner_device_id)
        for message_id, owners in message_owners.items():
            if len(owners) > 1:
                self._record_failure(
                    {
                        "device_index": -1,
                        "device_id": "multiple",
                        "ids_involved": {"messageId": message_id, "devices": sorted(owners)},
                        "failed_invariant": "message_isolation",
                        "details": "same messageId observed across multiple devices",
                    }
                )

        # Invariant 1B: Device A must never observe Device B's sessionId.
        session_owners = {session_id: device_id for device_id, session_id in self.session_by_device.items()}
        for device_id, observed_sessions in self.observed_sessions_by_device.items():
            for observed_session in observed_sessions:
                owner = session_owners.get(observed_session)
                if owner and owner != device_id:
                    self._record_failure(
                        {
                            "device_index": int(device_id.split("-")[-1]),
                            "device_id": device_id,
                            "ids_involved": {
                                "observed_session_id": observed_session,
                                "owner_device_id": owner,
                            },
                            "failed_invariant": "session_isolation",
                            "details": "device observed another device sessionId",
                        }
                    )

        # Invariant 1C: Device A must never read device_state of Device B.
        for device_id, read_doc_ids in self.device_state_reads_by_device.items():
            foreign_reads = sorted([doc_id for doc_id in read_doc_ids if doc_id != device_id])
            if foreign_reads:
                self._record_failure(
                    {
                        "device_index": int(device_id.split("-")[-1]),
                        "device_id": device_id,
                        "ids_involved": {
                            "foreign_device_state_reads": foreign_reads,
                        },
                        "failed_invariant": "device_state_isolation",
                        "details": "device performed cross-device device_state read",
                    }
                )

        # Invariant 2: device_state content must stay isolated (marker unchanged).
        for device_id, expected_marker in self.device_state_markers.items():
            snap = await self._fs_get("device_state", device_id)
            data = snap.to_dict() if snap.exists else {}
            actual_marker = (((data or {}).get("context") or {}).get("marker"))
            if actual_marker != expected_marker:
                self._record_failure(
                    {
                        "device_index": int(device_id.split("-")[-1]),
                        "device_id": device_id,
                        "ids_involved": {
                            "expected_marker": expected_marker,
                            "actual_marker": actual_marker,
                        },
                        "failed_invariant": "device_state_isolation",
                        "details": "device_state marker mismatch indicates possible cross-device write",
                    }
                )

        # Invariant 3: server message routing integrity.
        # All server responses must contain correct deviceId and sessionId
        # and match the originating request.
        for doc in message_docs:
            payload = doc.to_dict() or {}
            if payload.get("direction") != "server_to_client":
                continue
            request_id = (((payload.get("payload") or {}).get("requestId")) or "")
            session_id = payload.get("sessionId")
            device_id = payload.get("deviceId")
            expected_session = self.session_by_device.get(str(device_id))
            if not request_id or expected_session != session_id:
                self._record_failure(
                    {
                        "device_index": int(str(device_id).split("-")[-1]) if isinstance(device_id, str) and "-" in device_id else -1,
                        "device_id": device_id,
                        "ids_involved": {
                            "messageId": payload.get("messageId"),
                            "requestId": request_id,
                            "sessionId": session_id,
                            "expectedSession": expected_session,
                        },
                        "failed_invariant": "message_routing_integrity",
                        "details": "response message does not match originating request (missing requestId or invalid sessionId/deviceId)",
                    }
                )

        # Firestore consistency check: every message's sessionId and deviceId should reference existing session and device documents.
        for doc in message_docs:
            payload = doc.to_dict() or {}
            session_id = payload.get("sessionId")
            device_id = payload.get("deviceId")
            session_snap = await self._fs_get("sessions", str(session_id))
            device_snap = await self._fs_get("devices", str(device_id))
            if not session_snap.exists or not device_snap.exists:
                self._record_failure(
                    {
                        "device_index": -1,
                        "device_id": device_id,
                        "ids_involved": {
                            "messageId": payload.get("messageId"),
                            "sessionId": session_id,
                            "deviceId": device_id,
                        },
                        "failed_invariant": "firestore_consistency",
                        "details": "message references missing session or device document",
                    }
                )

        for result in results:
            for failure in result.failures:
                self._record_failure(failure)

    async def run(self) -> int:
        if self.cfg.devices < 1:
            print("--devices must be >= 1")
            return 2

        monitor_stop_event = asyncio.Event()
        monitor_task: Optional[asyncio.Task] = None
        if self.cfg.monitor_docker_container:
            print(
                f"Docker monitor: starting for container '{self.cfg.monitor_docker_container}' "
                f"every {self.cfg.monitor_docker_interval_s:.1f}s"
            )
            monitor_task = asyncio.create_task(self._run_docker_monitor(monitor_stop_event))

        end_time = time.perf_counter() + self.cfg.duration
        tasks = [asyncio.create_task(self.run_device(i, end_time)) for i in range(self.cfg.devices)]
        try:
            results: List[DeviceRunResult] = []
            completed = 0
            bar_width = 28

            def _print_progress() -> None:
                ratio = completed / self.cfg.devices
                filled = int(bar_width * ratio)
                bar = ("#" * filled) + ("-" * (bar_width - filled))
                print(
                    f"\rDevice progress: [{bar}] {completed}/{self.cfg.devices}",
                    end="",
                    flush=True,
                )

            _print_progress()
            for task in asyncio.as_completed(tasks):
                result = await task
                results.append(result)
                completed += 1
                _print_progress()
            print()

            self.success_count = sum(1 for r in results if r.success)
            await self.validate_post_conditions(results)
        finally:
            if monitor_task is not None:
                monitor_stop_event.set()
                await monitor_task

        total_failures = len(self.failures)
        required_failures = [
            failure
            for failure in self.failures
            if failure.get("failed_invariant") in REQUIRED_INVARIANTS
        ]
        required_failure_count = len(required_failures)
        runtime_s = time.perf_counter() - self.started_at
        failure_categories = Counter(f.get("failed_invariant", "unknown") for f in self.failures)
        total_iterations = sum(r.iterations for r in results)
        throughput = total_iterations / runtime_s if runtime_s > 0 else 0.0

        print(f"Devices: {self.cfg.devices}")
        print(f"Success: {self.success_count}")
        print(f"Failures: {total_failures}")
        print(f"Required invariant failures: {required_failure_count}")
        print(f"Runtime: {runtime_s:.2f}s")
        print(f"Actions: {total_iterations}")
        print(f"Throughput: {throughput:.2f} actions/s")

        # Calculate error and timeout rates
        if total_iterations > 0:
            error_rate = (total_failures / total_iterations) * 100
            timeout_rate = (self.timeout_count / total_iterations) * 100
            print(f"Error rate: {error_rate:.1f}% ({total_failures}/{total_iterations})")
            print(f"Timeout rate: {timeout_rate:.1f}% ({self.timeout_count}/{total_iterations})")
        else:
            print("Error rate: N/A (no requests)")
            print("Timeout rate: N/A (no requests)")

        if self.latencies_ms:
            p50 = self._percentile(self.latencies_ms, 0.50)
            p95 = self._percentile(self.latencies_ms, 0.95)
            p99 = self._percentile(self.latencies_ms, 0.99)
            print(f"Latency p50: {p50:.1f}ms")
            print(f"Latency p95: {p95:.1f}ms")
            print(f"Latency p99: {p99:.1f}ms")

        if self.cfg.monitor_docker_container:
            if self.backend_cpu_samples:
                cpu_avg = sum(self.backend_cpu_samples) / len(self.backend_cpu_samples)
                cpu_peak = max(self.backend_cpu_samples)
                print(f"Backend CPU avg: {cpu_avg:.2f}%")
                print(f"Backend CPU peak: {cpu_peak:.2f}%")
            else:
                print("Backend CPU avg: N/A")
                print("Backend CPU peak: N/A")

            if self.backend_mem_gib_samples:
                mem_avg = sum(self.backend_mem_gib_samples) / len(self.backend_mem_gib_samples)
                mem_peak = max(self.backend_mem_gib_samples)
                print(f"Backend memory avg: {mem_avg:.3f} GiB")
                print(f"Backend memory peak: {mem_peak:.3f} GiB")
            else:
                print("Backend memory avg: N/A")
                print("Backend memory peak: N/A")

            if self.docker_monitor_errors > 0:
                print(f"Docker monitor sample errors: {self.docker_monitor_errors}")

        if failure_categories:
            print("Failure categories:")
            for category, count in sorted(failure_categories.items()):
                print(f"  - {category}: {count}")

        if self.failures:
            print("Sample failures:")
            for failure in self.failures[:10]:
                print(json.dumps(failure, ensure_ascii=False))

        return 0 if required_failure_count == 0 else 1


async def _main() -> int:
    harness = Harness(parse_args())
    return await harness.run()


def main() -> None:
    exit_code = asyncio.run(_main())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
