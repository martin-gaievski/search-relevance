#!/bin/bash

# Local Ollama Stop Script
# This script stops the local Ollama services and bridge
# Usage: ./scripts/stop-local-ollama.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OLLAMA_DIR="$PROJECT_ROOT/.ollama"

echo "=== Stopping Local Ollama Environment ==="

# Stop bridge server
if [ -f "$OLLAMA_DIR/bridge.pid" ]; then
    BRIDGE_PID=$(cat "$OLLAMA_DIR/bridge.pid")
    if kill -0 $BRIDGE_PID 2>/dev/null; then
        echo "Stopping Ollama bridge (PID: $BRIDGE_PID)..."
        kill $BRIDGE_PID
        rm "$OLLAMA_DIR/bridge.pid"
        echo "Ollama bridge stopped"
    else
        echo "Bridge PID file exists but process not running"
        rm "$OLLAMA_DIR/bridge.pid"
    fi
else
    echo "No bridge PID file found"
fi

# Stop Ollama service (if we started it)
if [ -f "$OLLAMA_DIR/ollama.pid" ]; then
    OLLAMA_PID=$(cat "$OLLAMA_DIR/ollama.pid")
    if kill -0 $OLLAMA_PID 2>/dev/null; then
        echo "Stopping Ollama service (PID: $OLLAMA_PID)..."
        kill $OLLAMA_PID
        rm "$OLLAMA_DIR/ollama.pid"
        echo "Ollama service stopped"
    else
        echo "Ollama PID file exists but process not running"
        rm "$OLLAMA_DIR/ollama.pid"
    fi
else
    echo "No Ollama PID file found (may be running as system service)"
fi

# Kill any remaining processes by name (backup cleanup)
echo "Checking for remaining processes..."

# Kill bridge processes
BRIDGE_PROCS=$(pgrep -f "ollama_bridge.py" || true)
if [ ! -z "$BRIDGE_PROCS" ]; then
    echo "Killing remaining bridge processes: $BRIDGE_PROCS"
    pkill -f "ollama_bridge.py" || true
fi

# Kill Ollama processes (but only if we started them - be careful not to kill system Ollama)
if [ -f "$OLLAMA_DIR/ollama.pid" ] || pgrep -f "ollama serve" > /dev/null; then
    OLLAMA_PROCS=$(pgrep -f "ollama serve" || true)
    if [ ! -z "$OLLAMA_PROCS" ]; then
        echo "Found Ollama serve processes: $OLLAMA_PROCS"
        echo "Note: Not killing Ollama processes automatically"
        echo "If you want to stop Ollama completely, run: pkill -f 'ollama serve'"
    fi
fi

# Check final status
echo ""
echo "Final status check:"

if curl -s http://localhost:8080/v1/models > /dev/null 2>&1; then
    echo "Bridge still responding on port 8080"
else
    echo "Bridge stopped (port 8080 free)"
fi

if curl -s http://localhost:11434/api/version > /dev/null 2>&1; then
    echo "Ollama still running on port 11434 (may be system service)"
else
    echo "Ollama stopped (port 11434 free)"
fi

echo ""
echo "🎉 Local Ollama Environment Cleanup Complete!"
echo ""
echo "Cleaned up:"
echo "  • Ollama bridge (port 8080)"
echo "  • Local Ollama processes"
echo "  • PID files"
echo ""
echo "To restart: ./scripts/setup-local-ollama.sh"
echo ""
