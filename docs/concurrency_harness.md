# Concurrency + State Isolation Harness

This runbook documents the current harness implementation in `tools/seed_firestore.py` and `tools/concurrency_test.py`.

## 1) Scope

- Local backend only (`main/xiaozhi-server`)
- Firestore emulator only (`localhost:8080` by default)
- Deterministic seed data and deterministic randomization
- Concurrent device lifecycle simulation over WebSocket
- Isolation and integrity checks with CI-friendly exit codes

## 2) Prerequisites



1. Install Firebase CLI:

```bash
npm install -g firebase-tools
```

2. Start Firestore emulator:

```bash
firebase emulators:start --only firestore --project baby-milu-local
```

3. Setup and start the server backend (see `docs/Deployment.md` for reference):

4. Ensure backend and tools point to emulator:

Linux/macOS:

```bash
export FIRESTORE_EMULATOR_HOST=localhost:8080
```

Windows PowerShell:

```powershell
$env:FIRESTORE_EMULATOR_HOST = "localhost:8080"
```




## 3) Run Commands

### Using `make` (Linux/macOS)



Seed and run harness:

```bash
make seed_firestore
make concurrency_test
```

With overrides:

```bash
make seed_firestore DEVICES=100 SEED=42
make concurrency_test DEVICES=100 SEED=42 RAMP=5 DURATION=60
```

### Using direct Python commands (PowerShell or any shell)

### Seed deterministic Firestore state

```powershell
python tools/seed_firestore.py --devices 20 --seed 42 --project-id baby-milu-local --clear-existing
```

### Run concurrency harness (default monitor enabled)

```powershell
python tools/concurrency_test.py --devices 20 --seed 42 --ramp 5 --duration 60 --ws-url "ws://127.0.0.1:8000/xiaozhi/v1/" --project-id baby-milu-local
```

### Optional N=100 run

```powershell
python tools/concurrency_test.py --devices 100 --seed 42 --ramp 5 --duration 60 --handshake-retries 3 --handshake-backoff 0.4 --max-concurrent-handshakes 3 --ws-url "ws://127.0.0.1:8000/xiaozhi/v1/" --project-id baby-milu-local
```

## 4) CLI Reference

### Seeding (`tools/seed_firestore.py`)

```bash
python tools/seed_firestore.py \
  --devices 20 \
  --seed 42 \
  --project-id baby-milu-local \
  --emulator-host localhost:8080 \
  --clear-existing
```

Options:

- `--devices`: number of devices to seed
- `--seed`: deterministic RNG seed
- `--project-id`: Firestore project namespace
- `--emulator-host`: Firestore emulator endpoint
- `--clear-existing`: clear seeded collections before writing new data

### Concurrency harness (`tools/concurrency_test.py`)

```bash
python tools/concurrency_test.py \
  --devices 20 \
  --seed 42 \
  --ramp 5 \
  --duration 60 \
  --ws-url ws://127.0.0.1:8000/xiaozhi/v1/ \
  --project-id baby-milu-local \
  --emulator-host localhost:8080 \
  --auth-token "" \
  --request-timeout 6 \
  --handshake-retries 2 \
  --handshake-backoff 0.5 \
  --max-concurrent-handshakes 4 \
  --monitor-docker-container xiaozhi-esp32-server \
  --monitor-docker-interval 1.0
```

Options:

- `--devices`: number of simulated devices
- `--seed`: deterministic RNG seed
- `--ramp`: start rate (devices/sec)
- `--duration`: test window (seconds)
- `--ws-url`: websocket endpoint
- `--project-id`: Firestore project namespace
- `--emulator-host`: Firestore emulator endpoint
- `--auth-token`: optional bearer token
- `--request-timeout`: recv timeout for hello and message waits
- `--handshake-retries`: retries for connect+hello
- `--handshake-backoff`: exponential retry backoff base seconds
- `--max-concurrent-handshakes`: concurrent handshake cap to reduce burst pressure
- `--monitor-docker-container`: container name for automatic backend stats collection (empty to disable)
- `--monitor-docker-interval`: docker stats sampling interval in seconds

## 5) Current Harness Behavior

### Simulated lifecycle per device

1. Connect via websocket with `device-id` and `client-id` query params.
2. Send `hello`, wait for `session_id`.
3. Upsert `sessions` document.
4. Upsert own `device_state` with a per-device marker.
5. Send control message (`abort`), wait for `tts stop` response.
6. Persist request/response records into `messages`.
7. Update session `lastSeenAt`.
8. Repeat until test duration ends.

### Runtime observability

- Live device progress bar: `Device progress: [#####-----] x/N`
- Automatic docker monitor lifecycle:
  - starts at beginning of run
  - samples CPU/memory during run
  - stops at end and prints summary

### Output fields

- Devices, Success, Failures, Required invariant failures
- Runtime, Actions, Throughput
- Error rate, Timeout rate
- Latency p50/p95/p99 (if any successful action)
- Backend CPU avg/peak (if monitor enabled)
- Backend memory avg/peak (if monitor enabled)
- Failure categories and sample failure entries

## 6) Seeded Schema Contract

Collections written by seeder:

- `devices`
- `sessions`
- `messages`
- `device_state`

Relationship guarantees:

- `sessions.deviceId` references existing `devices.deviceId`
- `messages.sessionId` references existing `sessions.sessionId`
- `device_state` uses `deviceId` as document id (one state document per device)

## 7) Isolation Invariants

The harness validates invariants in the same categories used by `validate_post_conditions`:

1. Session isolation
  - One `messageId` must not be shared by multiple devices (so one device will never receive messages intended for another device).
  - Device A must never observe Device B's `sessionId` in received messages.
  - Device A must never read `device_state` documents owned by Device B.

2. Device-state isolation
  - Each device writes a unique marker in `device_state.context.marker`.
  - Post-run validation checks marker remains unchanged for that device to ensure that the device's state is unchanged.

3. Message routing integrity
  - For `server_to_client` messages, request linkage and session ownership must match the originating device/session mapping.

4. Firestore consistency (additional check)
  - Every run-generated message's `sessionId` and `deviceId` should reference existing `sessions` and `devices` documents.

Note:

- `lifecycle` failures are collected and reported, but are intentionally not part of `REQUIRED_INVARIANTS` in the current code.

## 8) Exit Codes

- `0`: all required invariants pass
- non-zero: one or more required invariants fail

## 9) Stretch: Scalability and Robustness Analysis

## A) Performance Snapshot

Recommended runs:

```powershell
# N=20
$env:FIRESTORE_EMULATOR_HOST="localhost:8080"; python tools/concurrency_test.py --devices 20 --seed 42 --ramp 5 --duration 60 --ws-url "ws://127.0.0.1:8000/xiaozhi/v1/" --project-id baby-milu-local

# N=100 (optional)
$env:FIRESTORE_EMULATOR_HOST="localhost:8080"; python tools/concurrency_test.py --devices 100 --seed 42 --ramp 5 --duration 60 --handshake-retries 3 --handshake-backoff 0.4 --max-concurrent-handshakes 3 --ws-url "ws://127.0.0.1:8000/xiaozhi/v1/" --project-id baby-milu-local
```

Observed results at docker stats sampling frequency of 1s:

- N=20
```powershell
Devices: 20
Success: 19
Failures: 1
Required invariant failures: 0
Runtime: 74.23s
Actions: 103
Throughput: 1.39 actions/s
Error rate: 1.0% (1/103)
Timeout rate: 2.9% (3/103)
Latency p50: 2.7ms
Latency p95: 4.5ms
Latency p99: 5.3ms
Backend CPU avg: 2.15%
Backend CPU peak: 8.80%
Backend memory avg: 2.843 GiB
Backend memory peak: 2.844 GiB
Failure categories:
  - lifecycle: 1
Sample failures:
{"device_index": 0, "device_id": "dev-0000", "failed_invariant": "lifecycle", "details": "device lifecycle failed: TimeoutError"}
```

- N=100
```powershell
Devices: 100
Success: 100
Failures: 0
Required invariant failures: 0
Runtime: 343.54s
Actions: 138
Throughput: 0.40 actions/s
Error rate: 0.0% (0/138)
Timeout rate: 10.1% (14/138)
Latency p50: 2.7ms
Latency p95: 4.6ms
Latency p99: 6.8ms
Backend CPU avg: 2.46%
Backend CPU peak: 9.38%
Backend memory avg: 2.385 GiB
Backend memory peak: 2.846 GiB
```

Based on current observed runs,

- CPU/memory are not saturated in either run (low avg/peak utilization).
- Response latency remains low (single-digit ms p99), so per-request processing is not the primary bottleneck.
- Throughput drops significantly from N=20 to N=100 while timeout rate rises (2.9% -> 10.1%).

This points to timeout behavior under higher concurrency rather than compute limits, potentially due to resource contention from multiple concurrent devices.

One optimization to try first:

- Improve lifecycle resilience and pacing first:
  - tune `--request-timeout`, --handshake-retries/backoff, and `--max-concurrent-handshakes` to reduce occurrence of timeouts at higher concurrency levels
  - investigate whether timeouts are concentrated in connect/hello phases or later message waits to further isolate bottlenecks and refine pacing strategy

What to measure in production for VM sizing confidence:

- stage-based timeout rate (connect, hello, first response, steady-state message) to understand where the timeouts are occurring and whether they align with the local test bottlenecks
- websocket/session setup p95/p99 latency to understand if initial handshake is the bottleneck
- active sessions and queueing/backpressure indicators to understand if resource contention is occurring at higher concurrency levels
- throughput per instance at fixed SLO targets to understand how many instances are needed to support target concurrency with acceptable timeout rates
- CPU/memory averages and peaks correlated with timeout spikes to understand if timeouts are more related to contention and queuing delays rather than pure compute resource limits.

## B) Observed Risks

- Timeout and final failure for concurrency tests can diverge:
    One device may have multiple timeout events (for example, connect timeout + hello timeout + message wait timeout) but only one final lifecycle failure, which can make it challenging to correlate timeout counts with user-visible failure rates. Additionally, the retry mechanism for connect/hello can reduce final lifecycle failures at the cost of increasing timeout events, which may be a necessary tradeoff but can complicate interpretation of timeout metrics as a leading indicator of user-visible issues.
  
- Lack of backpressure:
    Timeout is high (and further increased when N=100) while CPU/memory are low:
    This suggests that the bottleneck is not pure compute saturation but rather coordination, queuing, or contention issues that arise under higher concurrency. This may be due to multiple devices attempting to connect and send messages simultaneously, which can lead to resource contention or queuing delays that cause timeouts even though the CPU/memory are not fully utilized.


## C) Recommended Improvements

1. Add explicit admission control and queue-based backpressure at websocket ingress

- Why this matters:
  - At N=100, timeout rate increases while CPU/memory remain low, indicating queueing/coordination pressure rather than compute exhaustion.
- System change:
  - Introduce bounded ingress queues and admission limits for connect/hello/message processing.
  - Use overload policies (reject, defer, retry-after) instead of allowing silent timeout cascades.
- How to validate:
  - Track queue depth, enqueue wait, dequeue latency, and timeout rate under different concurrency levels.
  - Success criteria: lower timeout rate and steadier throughput at the same concurrency.

2. Decouple websocket response from Firestore writes with async buffering and batching

- Why this matters:
  - Per-message/session writes can amplify latency jitter under burst, even when compute is available. This can lead to timeouts that are not due to CPU limits but due to write amplification and contention on Firestore.
- System change:
  - Move message/session persistence to async buffered workers with batching or message queuing.
  - Keep websocket focused on session + response delivery.
- How to validate:
  - Compare end-to-end timeout rate and throughput before/after decoupling.
  - Monitor write queue lag and dropped writes (should remain bounded/zero at target load).

3. Add streaming partial responses to optimize user-perceived latency

- Why this matters:
  - Current completion waits for final stop marker; users benefit from earlier first token/chunk even when full completion is slower.
- System change:
  - Stream partial text/audio chunks over websocket or server-sent events and preserve an explicit end-of-response marker.
  - Introduce Time-to-First-Token (TTFT) as a first-class SLO, separate from full-response latency.
- How to validate:
  - Compare TTFT p50/p95/p99 and abandonment/timeout behavior under different device levels with and without streaming.
  - Success criteria: significantly lower TTFT without increasing error/timeout rates.

4. Adopt SLO-driven autoscaling and capacity policy

- Why this matters:
  - Average CPU is not predictive for this workload and timeouts rose before compute saturation.
- System change:
  - Scale on handshake latency, timeout rate, active websocket count, and queue depth.
  - Use multi-signal autoscaling and define a proactive policy for capacity planning.
  - May also consider historical load patterns and predictive scaling to handle expected spikes (e.g., morning/evening peaks).
- How to validate:
  - Run load sweeps (N=20, 50, 100, 200, ...) and derive per-instance safe concurrency envelopes.
  - Success criteria: predictable SLO compliance at target concurrency with minimal overprovisioning.

5. Database optimizations and sharding
- Why this matters:
  - When the number of devices increases, a single Firestore instance may not be able to handle the load efficiently, leading to increased latency and timeouts. Sharding the database can help distribute the load and reduce contention.
  - It is important to ensure that the sharding strategy uses a sharding key that provides good distribution of data and minimizes hotspots. For example, sharding by `device_id` or `session_id` could help ensure that related data is stored together while still distributing the load across multiple shards.
- System change:
  - Implement sharding in Firestore by partitioning collections based on a sharding key (e.g., `device_id`).
  - Update the application logic to route reads/writes to the appropriate shard based on the sharding key.
  - Use consistent hashing or a similar strategy to ensure even distribution of data across shards and to minimize the impact of adding/removing shards.
- How to validate:
  - Monitor latency, throughput, and timeout rates before and after sharding under increasing concurrency levels.
  - Success criteria: improved performance and reduced timeouts at higher concurrency levels compared to a non-sharded setup.

## Additional Notes

### Config Assumptions

- Firestore emulator is reachable at `localhost:8080` (or `FIRESTORE_EMULATOR_HOST` is set).
- Harness and seeder use the same Firestore project namespace (default: `baby-milu-local`).
- Backend websocket endpoint is reachable at `ws://127.0.0.1:8000/xiaozhi/v1/`.
- Backend container name for auto-monitoring is `xiaozhi-esp32-server` unless overridden with `--monitor-docker-container`.
- Docker CLI is installed and accessible on PATH when docker monitoring is enabled.
- Load test control flow expects server responses that include a `tts` message with `state=stop` for request completion.
- `lifecycle` failures are reported, but pass/fail exit code is based on `REQUIRED_INVARIANTS` (which can be configured in `concurrency_test.py`).

### How devices are seeded

- Seeding is deterministic by `--seed` and `--devices`.
- IDs are derived directly from the loop index `idx` in `seed_firestore.py` (index `idx` controls all primary seed identifiers for that row):
  - `device_id = f"dev-{idx:04d}"`
  - `session_id = f"seed-sess-{idx:04d}"`
  - `message_id = f"seed-msg-{idx:04d}"`
  - `user_id = f"user-{idx // 4:04d}"` (4 devices per user bucket)
- Examples:
  - `idx=0` -> `device_id=dev-0000`, `session_id=seed-sess-0000`, `message_id=seed-msg-0000`, `user_id=user-0000`
  - `idx=3` -> `device_id=dev-0003`, `session_id=seed-sess-0003`, `message_id=seed-msg-0003`, `user_id=user-0000`
  - `idx=4` -> `device_id=dev-0004`, `session_id=seed-sess-0004`, `message_id=seed-msg-0004`, `user_id=user-0001`
  - `idx=17` -> `device_id=dev-0017`, `session_id=seed-sess-0017`, `message_id=seed-msg-0017`, `user_id=user-0004`
- For each seeded device, the seeder writes one document each to:
  - `devices`
  - `sessions`
  - `messages`
  - `device_state`
- `device_state` uses `deviceId` as document id (one state document per device).
- Timestamp base is deterministic (`2026-01-01T00:00:00Z`) with per-device second offset.
- `--clear-existing` deletes existing docs in `devices`, `sessions`, `messages`, and `device_state` before writing fresh seed data.


### AI Usage Disclosure

AI assistance was used for:

- deploying and debugging the Firestore emulator and local server environment
- brainstorming the design and structure of the concurrency harness
- autocompleting code snippets based on provided context and outlines
- refining language and formatting for clarity and consistency in the documentation

### Design note

Tradeoffs:

- Simplicity and determinism of seed data over real-world randomness:
  
    Seeder IDs and timestamps are index-derived and repeatable, which improves debuggability and reproducibility. However, real-world data may have more variability in IDs, timestamps, and user/device distributions, which could expose different edge cases (such as hotspots).

- Simple ramp and duration control over complex traffic patterns:

    The harness uses a linear ramp-up of devices and fixed test duration for simplicity.However, real-world traffic may have very different traffic patterns (e.g., sudden spikes, long tails) that could affect performance and bottlenecks differently than a steady ramp and run. Additionally, different devices may have different durations and message frequencies, which is not captured by the current uniform device behavior.

- Simplicity of completion signal over protocol flexibility:

    Request completion is keyed on receiving `tts` with `state=stop`. This gives a clear stop condition but can classify some alternate server behaviors as timeouts.
  
- Controlled handshake burst handling over immediate full parallelism:

    `--max-concurrent-handshakes` reduces connection spikes and handshake failures. Nonetheless, it can delay the ramp-up of devices and may not perfectly simulate a real-world scenario where many devices could attempt to connect simultaneously.

- Discrete sampling of docker stats observability instead of continuous monitoring:

    The current implementation samples CPU/memory at a fixed interval (e.g., 1s) rather than continuously. This provides a performance snapshot with low overhead, but may miss short-lived spikes or fluctuations that could be relevant for understanding bottlenecks and resource contention under load.


Scalability recommendations:

- Prioritize timeout-path optimization before VM up-sizing:

  In measured runs, CPU and memory are low while timeout rate increases at N=100 and throughput drops. This indicates coordination/queueing pressure before compute saturation.

- Scale websocket handling horizontally with deterministic connection routing:

  Use multiple stateless gateway instances and route connections by `device_id` hash (or sticky session) to reduce cross-instance session churn and improve cache locality.

- Decouple hot-path request handling from Firestore write amplification:

  Move firestore write to an async write pipeline (buffer + batch flush), so websocket response path is not blocked by per-message document writes.

- Partition state and traffic by device_id ranges:

  Shard device by `device_id` hash to avoid concentrated hotspots in application workers and storage access patterns.