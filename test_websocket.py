#!/usr/bin/env python3
"""Simple WebSocket connection test."""

import asyncio
import json
import sys

try:
    import websockets
except ImportError:
    print("❌ websockets not installed. Run: pip install websockets")
    sys.exit(1)


async def test_websocket(url):
    """Test WebSocket connection and handshake."""
    print(f"🔗 Connecting to {url}")
    
    try:
        async with websockets.connect(url) as ws:
            print("✓ Connected to WebSocket server")
            
            # Send hello message
            hello = {
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
            
            print("📤 Sending hello message...")
            await ws.send(json.dumps(hello))
            
            print("👂 Waiting for response (5 second timeout)...")
            try:
                response = await asyncio.wait_for(ws.recv(), timeout=5.0)
                print(f"📥 Received: {response[:200]}")
                
                try:
                    data = json.loads(response)
                    if "session_id" in data:
                        print(f"✓ ✓ ✓ SUCCESS! Got session_id: {data['session_id']}")
                        return True
                    else:
                        print(f"⚠️  Response missing session_id: {data}")
                        return False
                except json.JSONDecodeError:
                    print(f"⚠️ Response is not JSON: {response}")
                    return False
                    
            except asyncio.TimeoutError:
                print("❌ Timeout waiting for hello response (5s)")
                return False
    
    except ConnectionRefusedError:
        print(f"❌ Connection refused - server not running on {url}")
        return False
    
    except OSError as e:
        print(f"❌ Network error: {e}")
        return False
    
    except Exception as e:
        print(f"❌ Error: {type(e).__name__}: {e}")
        return False


async def main():
    url =  "ws://127.0.0.1:8000/xiaozhi/v1/"
    success = await test_websocket(url)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
