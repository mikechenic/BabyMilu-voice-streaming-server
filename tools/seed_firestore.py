#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import re
from typing import Iterable, List


UTC = timezone.utc
CONFIG_FILE = Path(__file__).resolve().parents[1] / "main" / "xiaozhi-server" / "data" / ".config.yaml"


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
class SeedConfig:
    devices: int
    seed: int
    project_id: str
    emulator_host: str
    clear_existing: bool


def parse_args() -> SeedConfig:
    parser = argparse.ArgumentParser(
        description="Seed Firestore emulator with deterministic BabyMilu device/session data."
    )
    parser.add_argument("--devices", type=int, default=20, help="Number of devices to seed.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic seed.")
    parser.add_argument(
        "--project-id",
        default="baby-milu-local",
        help="Firestore project id used by emulator client.",
    )
    parser.add_argument(
        "--emulator-host",
        default=_default_emulator_host(),
        help="Firestore emulator host, e.g. localhost:8080.",
    )
    parser.add_argument(
        "--clear-existing",
        action="store_true",
        help="Delete documents in seeded collections before writing new seed data.",
    )
    args = parser.parse_args()
    return SeedConfig(
        devices=args.devices,
        seed=args.seed,
        project_id=args.project_id,
        emulator_host=args.emulator_host,
        clear_existing=args.clear_existing,
    )


def make_client(cfg: SeedConfig):
    try:
        from google.cloud import firestore  # type: ignore[reportMissingImports]
    except ImportError as exc:
        raise RuntimeError(
            "google-cloud-firestore is required. Install dependencies from main/xiaozhi-server/requirements.txt"
        ) from exc

    os.environ["FIRESTORE_EMULATOR_HOST"] = cfg.emulator_host
    return firestore.Client(project=cfg.project_id)


def chunked(seq: List[str], chunk_size: int = 400) -> Iterable[List[str]]:
    for idx in range(0, len(seq), chunk_size):
        yield seq[idx : idx + chunk_size]


def clear_collection(client, collection: str) -> int:
    docs = list(client.collection(collection).stream())
    deleted = 0
    for batch_docs in chunked([doc.id for doc in docs]):
        batch = client.batch()
        for doc_id in batch_docs:
            batch.delete(client.collection(collection).document(doc_id))
            deleted += 1
        batch.commit()
    return deleted


def seed(cfg: SeedConfig) -> None:
    if cfg.devices < 1:
        raise ValueError("--devices must be >= 1")

    client = make_client(cfg)
    rng = random.Random(cfg.seed)
    base_time = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)

    collections = ["devices", "sessions", "messages", "device_state"]
    if cfg.clear_existing:
        for col in collections:
            deleted = clear_collection(client, col)
            print(f"Cleared {deleted} docs from {col}")

    batch = client.batch()
    writes = 0

    for idx in range(cfg.devices):
        device_id = f"dev-{idx:04d}"
        user_id = f"user-{idx // 4:04d}"
        session_id = f"seed-sess-{idx:04d}"
        message_id = f"seed-msg-{idx:04d}"
        created_at = base_time + timedelta(seconds=idx)
        status = "active" if rng.random() > 0.15 else "inactive"
        mode = "idle" if idx % 2 == 0 else "chat"

        device_doc = {
            "deviceId": device_id,
            "userId": user_id,
            "createdAt": created_at,
            "status": status,
        }
        session_doc = {
            "sessionId": session_id,
            "deviceId": device_id,
            "connectedAt": created_at,
            "lastSeenAt": created_at,
        }
        message_doc = {
            "messageId": message_id,
            "sessionId": session_id,
            "deviceId": device_id,
            "direction": "client_to_server",
            "payload": {
                "kind": "seed",
                "index": idx,
                "seed": cfg.seed,
            },
            "timestamp": created_at,
        }
        state_doc = {
            "deviceId": device_id,
            "currentMode": mode,
            "context": {
                "seed": cfg.seed,
                "index": idx,
                "owner": user_id,
            },
            "updatedAt": created_at,
        }

        batch.set(client.collection("devices").document(device_id), device_doc)
        batch.set(client.collection("sessions").document(session_id), session_doc)
        batch.set(client.collection("messages").document(message_id), message_doc)
        # device_state uniqueness is enforced by using deviceId as doc id.
        batch.set(client.collection("device_state").document(device_id), state_doc)
        writes += 4

        if writes >= 400:
            batch.commit()
            batch = client.batch()
            writes = 0

    if writes > 0:
        batch.commit()

    print("Seed complete")
    print(f"Devices: {cfg.devices}")
    print(f"Seed: {cfg.seed}")
    print(f"Emulator: {cfg.emulator_host}")
    print("Collections: devices, sessions, messages, device_state")


if __name__ == "__main__":
    seed(parse_args())
