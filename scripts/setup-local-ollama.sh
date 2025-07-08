#!/bin/bash

# Local Ollama Setup Script  
# This script sets up the same Ollama environment as the GitHub Action for local testing
# Usage: ./scripts/setup-local-ollama.sh [MODEL_NAME]
# Models: phi3:mini (default), llama3.2:3b, qwen2.5:7b, llama3.1:8b, mistral:7b

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OLLAMA_DIR="$PROJECT_ROOT/.ollama"

# Model selection
MODEL_CHOICE="${1:-${OLLAMA_MODEL_CHOICE:-phi3:mini}}"

# Model configurations (Ollama model names)
get_model_info() {
    local model="$1"
    local field="$2"
    
    case "$model" in
        "phi3:mini")
            case "$field" in
                "name") echo "phi3:mini" ;;
                "size") echo "~2.3GB" ;;
                "desc") echo "Microsoft Phi-3 Mini - Excellent balance" ;;
            esac
            ;;
        "llama3.2:3b")
            case "$field" in
                "name") echo "llama3.2:3b" ;;
                "size") echo "~2.0GB" ;;
                "desc") echo "Llama 3.2 3B - Good for low RAM" ;;
            esac
            ;;
        "qwen2.5:7b")
            case "$field" in
                "name") echo "qwen2.5:7b" ;;
                "size") echo "~4.1GB" ;;
                "desc") echo "Qwen2.5 7B - Excellent reasoning" ;;
            esac
            ;;
        "llama3.1:8b")
            case "$field" in
                "name") echo "llama3.1:8b" ;;
                "size") echo "~4.6GB" ;;
                "desc") echo "Llama 3.1 8B - High quality" ;;
            esac
            ;;
        "mistral:7b")
            case "$field" in
                "name") echo "mistral:7b" ;;
                "size") echo "~4.1GB" ;;
                "desc") echo "Mistral 7B - Reasoning expert" ;;
            esac
            ;;
        *)
            return 1
            ;;
    esac
}

# Validate model choice
if ! get_model_info "$MODEL_CHOICE" "name" >/dev/null 2>&1; then
    echo "❌ Unknown model: $MODEL_CHOICE"
    echo ""
    echo "Available models:"
    echo "  phi3:mini    - Microsoft Phi-3 Mini (~2.3GB) - ⭐ Recommended balance"
    echo "  llama3.2:3b - Llama 3.2 3B (~2.0GB) - ✅ Good for low RAM"
    echo "  qwen2.5:7b  - Qwen2.5 7B (~4.1GB) - 🧠 Excellent reasoning"
    echo "  llama3.1:8b - Llama 3.1 8B (~4.6GB) - 🏆 High quality"
    echo "  mistral:7b  - Mistral 7B (~4.1GB) - 🎓 Reasoning expert"
    echo ""
    echo "Usage: ./scripts/setup-local-ollama.sh [MODEL_NAME]"
    echo "Example: ./scripts/setup-local-ollama.sh phi3:mini"
    exit 1
fi

MODEL_NAME="$(get_model_info "$MODEL_CHOICE" "name")"
MODEL_SIZE="$(get_model_info "$MODEL_CHOICE" "size")"
MODEL_DESC="$(get_model_info "$MODEL_CHOICE" "desc")"

echo "=== Setting up Local Ollama Environment ==="
echo "Model: $MODEL_CHOICE ($MODEL_DESC)"
echo "Size: $MODEL_SIZE"
echo ""

# Create Ollama directory
mkdir -p "$OLLAMA_DIR"
cd "$OLLAMA_DIR"

# Check if Ollama is installed
if ! command -v ollama &> /dev/null; then
    echo "Installing Ollama..."
    curl -fsSL https://ollama.com/install.sh | sh
    echo "✅ Ollama installed successfully"
else
    echo "✅ Ollama already installed"
fi

# Check if Ollama service is already running
if curl -s http://localhost:11434/api/version > /dev/null 2>&1; then
    echo "✅ Ollama service already running"
else
    echo "Starting Ollama service..."
    ollama serve &
    OLLAMA_PID=$!
    echo $OLLAMA_PID > "$OLLAMA_DIR/ollama.pid"
    
    # Wait for Ollama to start
    echo "Waiting for Ollama to start..."
    for i in {1..30}; do
        if curl -s http://localhost:11434/api/version > /dev/null 2>&1; then
            echo "✅ Ollama service started successfully"
            break
        fi
        if [ $i -eq 30 ]; then
            echo "❌ Ollama failed to start within 1 minute"
            exit 1
        fi
        echo "  Attempt $i/30: Still waiting..."
        sleep 2
    done
fi

# Check if model is already available
if ollama list | grep -q "$MODEL_NAME"; then
    echo "✅ Model $MODEL_NAME already available"
else
    echo "Pulling model $MODEL_NAME ($MODEL_SIZE)..."
    echo "This may take several minutes depending on your internet connection..."
    ollama pull "$MODEL_NAME"
    echo "✅ Model $MODEL_NAME pulled successfully"
fi

# Verify model is available
echo ""
echo "Available models:"
ollama list

# Test Ollama API
echo ""
echo "Testing Ollama API..."
curl -s -X POST http://localhost:11434/api/chat \
  -H "Content-Type: application/json" \
  -d "{
    \"model\": \"$MODEL_NAME\",
    \"messages\": [{\"role\": \"user\", \"content\": \"Hello! Are you working?\"}],
    \"stream\": false
  }" | jq -r '.message.content' 2>/dev/null || echo "API test completed"

# Create Ollama bridge (same as GitHub Actions)
echo ""
echo "Creating Ollama bridge for OpenAI compatibility..."
cat > "$OLLAMA_DIR/ollama_bridge.py" <<'EOF'
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import requests
import uuid
from datetime import datetime
import sys

class OllamaHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/v1/models':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            response = {
                "data": [
                    {
                        "id": "phi3:mini",
                        "object": "model",
                        "created": int(datetime.now().timestamp()),
                        "owned_by": "ollama"
                    }
                ]
            }
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        request_data = json.loads(post_data.decode('utf-8'))

        if self.path == '/v1/chat/completions':
            try:
                # Convert OpenAI format to Ollama format
                ollama_request = {
                    "model": "phi3:mini",
                    "messages": request_data.get("messages", []),
                    "stream": False
                }
                
                # Call Ollama API
                ollama_response = requests.post(
                    "http://localhost:11434/api/chat",
                    json=ollama_request,
                    timeout=30
                )
                
                if ollama_response.status_code == 200:
                    ollama_data = ollama_response.json()
                    
                    # Convert Ollama response to OpenAI format
                    openai_response = {
                        "id": f"chatcmpl-{uuid.uuid4().hex[:8]}",
                        "object": "chat.completion",
                        "created": int(datetime.now().timestamp()),
                        "model": "phi3:mini",
                        "choices": [
                            {
                                "index": 0,
                                "message": {
                                    "role": "assistant",
                                    "content": ollama_data.get("message", {}).get("content", "")
                                },
                                "finish_reason": "stop"
                            }
                        ],
                        "usage": {
                            "prompt_tokens": len(str(request_data.get("messages", []))),
                            "completion_tokens": len(ollama_data.get("message", {}).get("content", "")),
                            "total_tokens": len(str(request_data.get("messages", []))) + len(ollama_data.get("message", {}).get("content", ""))
                        }
                    }
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(openai_response).encode())
                else:
                    self.send_response(500)
                    self.end_headers()
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                self.send_response(500)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress log messages

if __name__ == '__main__':
    server = HTTPServer(('localhost', 8080), OllamaHandler)
    print("Ollama bridge server started on http://localhost:8080")
    server.serve_forever()
EOF

# Check if bridge is already running
if curl -s http://localhost:8080/v1/models > /dev/null 2>&1; then
    echo "⚠️  Bridge server already running on port 8080"
else
    echo "Starting Ollama bridge on http://localhost:8080..."
    python3 "$OLLAMA_DIR/ollama_bridge.py" > "$OLLAMA_DIR/bridge.log" 2>&1 &
    BRIDGE_PID=$!
    echo $BRIDGE_PID > "$OLLAMA_DIR/bridge.pid"
    
    # Wait for bridge to start
    sleep 3
    
    # Test the bridge
    echo "Testing Ollama bridge..."
    if curl -s http://localhost:8080/v1/models > /dev/null 2>&1; then
        echo "✅ Ollama bridge started successfully"
    else
        echo "❌ Failed to start Ollama bridge"
        exit 1
    fi
fi

echo ""
echo "🎉 Local Ollama Environment Ready!"
echo ""
echo "Server Info:"
echo "  Ollama API: http://localhost:11434"
echo "  Bridge API: http://localhost:8080 (OpenAI-compatible)"
echo "  Model: $MODEL_NAME"
echo "  Description: $MODEL_DESC"
echo "  Size: $MODEL_SIZE"
echo ""
echo "PIDs:"
if [ -f "$OLLAMA_DIR/ollama.pid" ]; then
    echo "  Ollama: $(cat $OLLAMA_DIR/ollama.pid)"
fi
if [ -f "$OLLAMA_DIR/bridge.pid" ]; then
    echo "  Bridge: $(cat $OLLAMA_DIR/bridge.pid)"
fi
echo ""
echo "Logs:"
echo "  Bridge: $OLLAMA_DIR/bridge.log"
echo ""
echo "Next Steps:"
echo "  1. Run LLM tests: ./scripts/test-llm-local.sh"
echo "  2. Stop services: ./scripts/stop-local-ollama.sh"
echo "  3. View bridge logs: tail -f $OLLAMA_DIR/bridge.log"
echo ""
