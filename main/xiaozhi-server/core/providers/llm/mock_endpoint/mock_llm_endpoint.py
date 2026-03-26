#!/usr/bin/env python3
"""Local mock LLM endpoint for high-ramp/concurrency testing.

Implements a minimal OpenAI Responses-compatible streaming endpoint so the
existing openai provider can run without external LLM latency/rate limits.
"""

from __future__ import annotations

import argparse
import json
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict


class MockLLMHandler(BaseHTTPRequestHandler):
    server_version = "MockLLM/0.1"

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            parsed = json.loads(raw.decode("utf-8"))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _sse_write(self, event_name: str, payload: Dict[str, Any]) -> None:
        self.wfile.write(f"event: {event_name}\n".encode("utf-8"))
        self.wfile.write(f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8"))
        self.wfile.flush()

    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/healthz", "/health", "/"):
            self._send_json(HTTPStatus.OK, {"ok": True, "service": "mock-llm-endpoint"})
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found", "path": self.path})

    def do_POST(self) -> None:  # noqa: N802
        if self.path.endswith("/conversations"):
            self._handle_conversations()
            return
        if self.path.endswith("/responses"):
            self._handle_responses()
            return
        if self.path.endswith("/chat/completions"):
            self._handle_chat_completions()
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found", "path": self.path})

    def _handle_conversations(self) -> None:
        # Accept optional payload with items/system message and return a synthetic conversation id.
        _ = self._read_json()
        conv_id = f"conv-mock-{int(time.time() * 1000)}"
        self._send_json(
            HTTPStatus.OK,
            {
                "id": conv_id,
                "object": "conversation",
                "created_at": int(time.time()),
            },
        )

    def _extract_prompt(self, payload: Dict[str, Any]) -> str:
        data = payload.get("input", [])
        if not isinstance(data, list):
            return ""
        for item in reversed(data):
            if isinstance(item, dict) and item.get("role") == "user":
                content = item.get("content", "")
                if isinstance(content, str):
                    return content
        return ""

    def _handle_responses(self) -> None:
        payload = self._read_json()
        prompt = self._extract_prompt(payload)
        model = payload.get("model", "mock-model")
        response_id = f"resp-mock-{int(time.time() * 1000)}"

        response_text = "mock-response"
        if prompt:
            response_text = f"mock-response: {prompt[:80]}"

        # Stream path used by openai client.responses.stream
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        # OpenAI Responses stream expects a created event before deltas.
        self._sse_write(
            "response.created",
            {
                "type": "response.created",
                "response": {
                    "id": response_id,
                    "object": "response",
                    "created_at": int(time.time()),
                    "status": "in_progress",
                    "model": model,
                    "output": [],
                },
            },
        )

        self._sse_write(
            "response.output_item.added",
            {
                "type": "response.output_item.added",
                "response_id": response_id,
                "output_index": 0,
                "item": {
                    "type": "message",
                    "id": f"msg-{response_id}",
                    "role": "assistant",
                    "content": [],
                },
            },
        )

        self._sse_write(
            "response.content_part.added",
            {
                "type": "response.content_part.added",
                "response_id": response_id,
                "output_index": 0,
                "content_index": 0,
                "part": {
                    "type": "output_text",
                    "text": "",
                },
            },
        )

        chunks = [response_text[i : i + 12] for i in range(0, len(response_text), 12)] or ["mock-response"]
        for chunk in chunks:
            self._sse_write(
                "response.output_text.delta",
                {
                    "type": "response.output_text.delta",
                    "response_id": response_id,
                    "output_index": 0,
                    "content_index": 0,
                    "delta": chunk,
                    "model": model,
                },
            )
            time.sleep(0.01)

        self._sse_write(
            "response.completed",
            {
                "type": "response.completed",
                "response": {
                    "id": response_id,
                    "object": "response",
                    "created_at": int(time.time()),
                    "status": "completed",
                    "model": model,
                    "output": [
                        {
                            "type": "message",
                            "id": f"msg-{response_id}",
                            "role": "assistant",
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": response_text,
                                }
                            ],
                        }
                    ],
                },
                "model": model,
                "status": "completed",
            },
        )

    def _handle_chat_completions(self) -> None:
        payload = self._read_json()
        model = payload.get("model", "mock-model")
        response = {
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "mock-response"},
                    "finish_reason": "stop",
                }
            ],
        }
        self._send_json(HTTPStatus.OK, response)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[mock-llm] {self.address_string()} - {fmt % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a local mock LLM endpoint.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8099)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), MockLLMHandler)
    print(f"Mock LLM endpoint listening on http://{args.host}:{args.port}")
    print("Health check: GET /healthz")
    print("Responses endpoint: POST /api/paas/v4/responses")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
