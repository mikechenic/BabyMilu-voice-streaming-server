#!/usr/bin/env python3
"""Simple test script to verify ChatGLM API key and connectivity."""

import sys
import re
from pathlib import Path
import requests
import json

CONFIG_FILE = Path(__file__).resolve().parent / "main" / "xiaozhi-server" / "data" / ".config.yaml"


def read_api_key():
    """Read ChatGLM API key from .config.yaml"""
    try:
        content = CONFIG_FILE.read_text(encoding="utf-8")
    except FileNotFoundError:
        print(f"❌ Config file not found: {CONFIG_FILE}")
        return None
    
    # Look for api_key line: api_key: <value>
    match = re.search(r"(?m)^\s+api_key:\s*([^\n#]+)", content)
    if not match:
        print("❌ No api_key found in .config.yaml")
        return None
    
    api_key = match.group(1).strip()
    if api_key.startswith('"') and api_key.endswith('"'):
        api_key = api_key[1:-1]
    elif api_key.startswith("'") and api_key.endswith("'"):
        api_key = api_key[1:-1]
    
    if not api_key or "your" in api_key.lower():
        print("❌ API key is placeholder or not set properly")
        return None
    
    return api_key


def test_chatglm_api():
    """Test ChatGLM API connectivity and key validity."""
    api_key = read_api_key()
    if not api_key:
        return False
    
    print(f"✓ Found API key: {api_key[:20]}...")
    
    # ChatGLM API endpoints
    base_url = "https://open.bigmodel.cn/api/paas/v4"
    
    print(f"\n🔍 Testing ChatGLM API endpoint: {base_url}")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "glm-4",
        "messages": [
            {
                "role": "user",
                "content": "Hello, just testing API connectivity."
            }
        ],
        "max_tokens": 10,
        "temperature": 0.1
    }
    
    try:
        print("📤 Sending test request...")
        response = requests.post(
            f"{base_url}/chat/completions",
            headers=headers,
            json=payload,
            timeout=10
        )
        
        print(f"📥 Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if "choices" in data and len(data["choices"]) > 0:
                message = data["choices"][0].get("message", {}).get("content", "")
                print(f"✓ ✓ ✓ API KEY VALID! Server responded with: {message[:50]}...")
                return True
            else:
                print(f"⚠️  Unexpected response format: {json.dumps(data, indent=2)[:200]}")
                return False
        
        elif response.status_code == 401:
            print("❌ 401 Unauthorized - API key is invalid or expired")
            print(f"   Response: {response.text[:200]}")
            return False
        
        elif response.status_code == 404:
            print("❌ 404 Not Found - Endpoint or model not found")
            print(f"   Response: {response.text[:200]}")
            return False
        
        else:
            print(f"❌ Request failed with status {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            return False
    
    except requests.exceptions.Timeout:
        print("❌ Request timed out (10s) - cannot reach API endpoint")
        return False
    
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Connection error: {e}")
        print("   Check your internet connection and firewall")
        return False
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("ChatGLM API Key Validation Test")
    print("=" * 60)
    
    success = test_chatglm_api()
    
    print("\n" + "=" * 60)
    if success:
        print("✓ API KEY IS WORKING - You can use ChatGLMLLM")
        sys.exit(0)
    else:
        print("✗ API KEY IS NOT WORKING - See errors above")
        sys.exit(1)
