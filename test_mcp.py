#!/usr/bin/env python3
"""
Simple test script to verify MCP server functionality
"""
import json
import subprocess
import sys


def test_mcp_server():
    """Test if MCP server responds to basic requests"""
    print("🧪 Testing MCP server...")

    # Test request
    test_request = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}

    try:
        # Start the MCP server
        proc = subprocess.Popen(
            ["python3", "main.py"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Send test request
        request_json = json.dumps(test_request) + "\n"
        stdout, stderr = proc.communicate(input=request_json, timeout=10)

        print(f"STDOUT: {stdout}")
        print(f"STDERR: {stderr}")

        # Try to parse response
        if stdout.strip():
            try:
                response = json.loads(stdout.strip())
                if "result" in response:
                    tools = response["result"].get("tools", [])
                    print(f"✅ MCP server responded with {len(tools)} tools")
                    return True
                else:
                    print(f"❌ Unexpected response: {response}")
                    return False
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON response: {e}")
                return False
        else:
            print("❌ No response from MCP server")
            return False

    except subprocess.TimeoutExpired:
        print("❌ MCP server timeout")
        proc.kill()
        return False
    except Exception as e:
        print(f"❌ Error testing MCP server: {e}")
        return False


if __name__ == "__main__":
    success = test_mcp_server()
    sys.exit(0 if success else 1)
