@echo off
REM Instagram Downloader - Quick Start Script for Windows

echo.
echo 🚀 Instagram Video ^& Photo Downloader
echo =====================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python is not installed. Please install Python 3.7 or higher.
    echo Download from: https://www.python.org/downloads/
    pause
    exit /b 1
)

for /f "tokens=*" %%i in ('python --version') do set PYTHON_VERSION=%%i
echo ✅ Python found: %PYTHON_VERSION%
echo.

REM Check if ffmpeg is installed
where ffmpeg >nul 2>&1
if errorlevel 1 (
    echo ⚠️  ffmpeg is not installed. Some features may not work.
    echo Download from: https://ffmpeg.org/download.html
    echo.
)

REM Create downloads folder if it doesn't exist
if not exist "downloads" (
    mkdir downloads
    echo 📁 Created downloads folder
    echo.
)

REM Check if virtual environment exists
if not exist "venv" (
    echo 📦 Creating virtual environment...
    python -m venv venv
    echo ✅ Virtual environment created
    echo.
)

REM Activate virtual environment
echo 🔌 Activating virtual environment...
call venv\Scripts\activate.bat

REM Install requirements
echo 📥 Installing dependencies...
pip install -r requirements.txt --quiet
echo ✅ Dependencies installed
echo.

REM Start the application
echo 🎉 Starting application...
echo 📍 Open your browser and go to: http://localhost:5000
echo Press Ctrl+C to stop the server
echo.

python app.py

pause

