#!/bin/bash

# Instagram Downloader - Quick Start Script

echo "🚀 Instagram Video & Photo Downloader"
echo "=====================================\n"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.7 or higher."
    exit 1
fi

echo "✅ Python found: $(python3 --version)"

# Check if ffmpeg is installed
if ! command -v ffmpeg &> /dev/null; then
    echo "⚠️  ffmpeg is not installed. Some features may not work."
    echo "Install ffmpeg with: brew install ffmpeg (macOS) or apt-get install ffmpeg (Linux)"
fi

# Create downloads folder if it doesn't exist
if [ ! -d "downloads" ]; then
    mkdir -p downloads
    echo "📁 Created downloads folder"
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv/bin/activate

# Install requirements
echo "📥 Installing dependencies..."
pip install -r requirements.txt --quiet
echo "✅ Dependencies installed"

# Start the application
echo "\n🎉 Starting application..."
echo "📍 Open your browser and go to: http://localhost:5000"
echo "Press Ctrl+C to stop the server\n"

python3 app.py

