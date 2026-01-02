#!/bin/bash
# Local development script for Vanna AI Text-to-SQL app

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "Vanna AI Local Development Setup"
echo "=========================================="
echo ""

# Check if .env exists
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    echo "⚠ .env file not found. Generating from Terraform outputs..."
    python3 setup_local_env.py
    echo ""
fi

# Check if virtual environment exists
if [ ! -d "$SCRIPT_DIR/venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv venv
    echo "✓ Virtual environment created"
    echo ""
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate
echo "✓ Virtual environment activated"
echo ""

# Install/upgrade dependencies
echo "Installing/updating dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✓ Dependencies installed"
echo ""

# Check configuration
echo "Validating configuration..."
python3 -c "from config import Config; Config.validate(); print('✓ Configuration valid')" || {
    echo "✗ Configuration validation failed"
    echo "Please check your .env file and ensure all required values are set"
    exit 1
}
echo ""

# Start the application
echo "=========================================="
echo "Starting Vanna AI application..."
echo "=========================================="
echo "Application will be available at: http://localhost:8000"
echo "API docs at: http://localhost:8000/docs"
echo "Health check: http://localhost:8000/health"
echo ""
echo "Press Ctrl+C to stop"
echo ""

uvicorn app:app --reload --host 0.0.0.0 --port 8000
