#!/usr/bin/env python3
"""Quick compatibility test for core/providers/llm/mock_endpoint/mock_llm_endpoint.py."""

from __future__ import annotations

import argparse
import openai


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8099/api/paas/v4/")
    parser.add_argument("--model", default="glm-4-flash")
    args = parser.parse_args()

    client = openai.OpenAI(api_key="mock-key", base_url=args.base_url)

    output = []
    with client.responses.stream(
        model=args.model,
        input=[{"role": "user", "content": "ping"}],
        store=False,
    ) as stream:
        for event in stream:
            if event.type == "response.output_text.delta":
                output.append(event.delta or "")
            elif event.type == "response.completed":
                break

    text = "".join(output)
    print(f"received: {text}")
    if not text:
        print("mock endpoint test failed: empty output")
        return 1
    print("mock endpoint test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
