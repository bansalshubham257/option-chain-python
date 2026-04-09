from flask import Flask, render_template, request, jsonify, send_file
import os
import requests
from urllib.parse import urlparse
import subprocess
import json
from pathlib import Path
import logging
import sys
import time
from datetime import datetime, timedelta
import random
from collections import defaultdict
from threading import Lock

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max file size
DOWNLOAD_FOLDER = 'downloads'
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Rate limiting per user - REMOVED FOR UNLIMITED ACCESS
last_download_time = {}
MIN_SECONDS_BETWEEN_DOWNLOADS = 0  # No delay - unlimited downloads

# Track attempts per IP to detect rate-limiting
download_attempts = defaultdict(lambda: {'count': 0, 'last_reset': time.time(), 'rate_limited_until': 0})
MAX_ATTEMPTS_PER_MINUTE = 999  # No limit per minute
RATE_LIMIT_COOLDOWN = 0  # No cooldown

# Global request lock to prevent simultaneous Instagram requests
request_lock = Lock()
request_queue = []  # Queue of pending requests
request_in_progress = False

# Rotating user agents to avoid Instagram detection
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
]

def get_random_user_agent():
    """Get a random user agent from the list"""
    return random.choice(USER_AGENTS)

def check_rate_limit(client_ip):
    """Check if IP is currently rate-limited by Instagram"""
    current_time = time.time()
    attempt_info = download_attempts[client_ip]
    
    # Check if still in cooldown period
    if attempt_info['rate_limited_until'] > current_time:
        remaining = attempt_info['rate_limited_until'] - current_time
        return True, remaining
    
    # Reset rate-limited flag if cooldown expired
    attempt_info['rate_limited_until'] = 0
    
    # Reset count every minute
    if current_time - attempt_info['last_reset'] > 60:
        attempt_info['count'] = 0
        attempt_info['last_reset'] = current_time
    
    return False, 0

def mark_rate_limited(client_ip):
    """Mark IP as rate-limited for cooldown period"""
    current_time = time.time()
    download_attempts[client_ip]['rate_limited_until'] = current_time + RATE_LIMIT_COOLDOWN
    logger.warning(f"⚠️ IP {client_ip} marked as rate-limited until {datetime.fromtimestamp(download_attempts[client_ip]['rate_limited_until'])}")

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
logger.info("Starting Instagram Downloader App")

# Try to import instagrapi for Instagram handling
try:
    from instagrapi import Client
    INSTAGRAPI_AVAILABLE = True
    logger.info("✅ instagrapi available")
except ImportError:
    INSTAGRAPI_AVAILABLE = False
    logger.warning("⚠️ instagrapi not available")

# Alternative: Use yt-dlp which works well for Instagram
try:
    import yt_dlp
    YTDLP_AVAILABLE = True
    logger.info("✅ yt-dlp available")
except ImportError:
    YTDLP_AVAILABLE = False
    logger.error("❌ yt-dlp NOT available - downloads will fail")


@app.route('/')
def index():
    """Render the main page"""
    return render_template('index.html')


@app.route('/api/formats', methods=['POST'])
def get_formats():
    """Get available formats/qualities for an Instagram URL"""
    try:
        data = request.json
        instagram_url = data.get('url', '').strip()
        logger.info(f"📋 Fetching formats for: {instagram_url}")
        
        if not instagram_url:
            return jsonify({'error': 'Please provide an Instagram URL'}), 400
        
        if 'instagram.com' not in instagram_url:
            return jsonify({'error': 'Please provide a valid Instagram URL'}), 400
        
        if not YTDLP_AVAILABLE:
            return jsonify({'error': 'Service not available'}), 503
        
        formats_list = []
        
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'socket_timeout': 90,
                'http_headers': {
                    'User-Agent': get_random_user_agent(),  # Rotate user agents
                    'Accept-Language': 'en-US,en;q=0.9',
                },
                'retries': 3,
                'fragment_retries': 3,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                logger.info("📥 Extracting format info...")
                info = ydl.extract_info(instagram_url, download=False)
                
                # Get available formats
                if 'formats' in info:
                    seen_formats = set()
                    for fmt in info['formats']:
                        # Extract quality info
                        height = fmt.get('height', 'N/A')
                        width = fmt.get('width', 'N/A')
                        filesize = fmt.get('filesize', 0) or 0
                        ext = fmt.get('ext', 'unknown')
                        format_id = fmt.get('format_id', '')
                        vcodec = fmt.get('vcodec', 'none')
                        acodec = fmt.get('acodec', 'none')
                        
                        # Skip duplicate resolutions
                        quality_key = f"{height}_{width}_{vcodec}"
                        if quality_key in seen_formats:
                            continue
                        seen_formats.add(quality_key)
                        
                        # Only include video/image formats with good quality
                        if (height != 'N/A' and height > 0) or ext in ['jpg', 'png']:
                            filesize_mb = filesize / (1024 * 1024) if filesize else 0
                            
                            # Create quality label
                            if ext in ['jpg', 'png']:
                                quality_label = f"Photo ({width}x{height})"
                            else:
                                quality_label = f"{height}p Video"
                                if acodec != 'none':
                                    quality_label += " (with audio)"
                            
                            formats_list.append({
                                'format_id': format_id,
                                'quality': quality_label,
                                'height': height,
                                'width': width,
                                'type': ext,
                                'size_mb': round(filesize_mb, 2),
                                'has_audio': acodec != 'none',
                            })
                
                # Also add best and worst options
                formats_list.insert(0, {
                    'format_id': 'best',
                    'quality': 'Best Quality (Auto)',
                    'height': 'Auto',
                    'width': 'Auto',
                    'type': 'auto',
                    'size_mb': 'Unknown',
                    'has_audio': True,
                })
                
                formats_list.append({
                    'format_id': 'worst',
                    'quality': 'Smallest File Size',
                    'height': 'Auto',
                    'width': 'Auto',
                    'type': 'auto',
                    'size_mb': 'Unknown',
                    'has_audio': False,
                })
                
                # Remove duplicates
                unique_formats = []
                seen = set()
                for fmt in formats_list:
                    key = (fmt['format_id'], fmt['height'])
                    if key not in seen:
                        unique_formats.append(fmt)
                        seen.add(key)
                
                logger.info(f"✅ Found {len(unique_formats)} formats")
                
                return jsonify({
                    'success': True,
                    'formats': unique_formats,
                    'title': info.get('title', 'Instagram Media'),
                    'duration': info.get('duration', 0),
                })
                
        except Exception as e:
            logger.error(f"❌ Format extraction error: {str(e)}", exc_info=True)
            
            # If rate-limited or auth error, return default formats
            error_str = str(e).lower()
            if 'rate' in error_str or 'login required' in error_str or 'not available' in error_str:
                logger.info("🔄 Instagram rate-limited, using default formats...")
                return jsonify({
                    'success': True,
                    'formats': [
                        {
                            'format_id': 'best',
                            'quality': 'Best Quality (Auto)',
                            'height': 'Auto',
                            'width': 'Auto',
                            'type': 'auto',
                            'size_mb': 'Unknown',
                            'has_audio': True,
                            'note': 'Best available quality'
                        },
                        {
                            'format_id': 'best[height<=1080]',
                            'quality': '1080p Video (if available)',
                            'height': '1080',
                            'width': '1920',
                            'type': 'mp4',
                            'size_mb': '100-200',
                            'has_audio': True,
                        },
                        {
                            'format_id': 'best[height<=720]',
                            'quality': '720p Video (Recommended)',
                            'height': '720',
                            'width': '1280',
                            'type': 'mp4',
                            'size_mb': '50-100',
                            'has_audio': True,
                        },
                        {
                            'format_id': 'best[height<=480]',
                            'quality': '480p Video (Smaller)',
                            'height': '480',
                            'width': '854',
                            'type': 'mp4',
                            'size_mb': '20-50',
                            'has_audio': True,
                        },
                        {
                            'format_id': 'worst',
                            'quality': 'Smallest File Size',
                            'height': 'Minimal',
                            'width': 'Minimal',
                            'type': 'auto',
                            'size_mb': '2-10',
                            'has_audio': False,
                        }
                    ],
                    'title': 'Instagram Media',
                    'duration': 0,
                    'note': 'Instagram is rate-limiting. Using default quality options. Actual quality will be auto-selected.',
                })
            
            return jsonify({'error': f'Could not fetch formats: {str(e)}'}), 400
    
    except Exception as e:
        logger.error(f"❌ Error in get_formats: {str(e)}", exc_info=True)
        return jsonify({'error': f'Error: {str(e)}'}), 500


@app.route('/api/download', methods=['POST'])
def download():
    """Handle Instagram link downloads with format selection"""
    try:
        data = request.json
        instagram_url = data.get('url', '').strip()
        format_id = data.get('format_id', 'best')  # Get selected format
        
        client_ip = request.remote_addr
        logger.info(f"📥 Download request from {client_ip}: {instagram_url} | Format: {format_id}")
        
        # ⚠️ RATE LIMITING DISABLED - ALLOWING UNLIMITED DOWNLOADS
        # Note: Instagram will still rate-limit based on their own anti-bot detection
        
        current_time = time.time()
        
        if not instagram_url:
            logger.warning("❌ No URL provided")
            return jsonify({'error': 'Please provide an Instagram URL'}), 400
        
        # Validate Instagram URL
        if 'instagram.com' not in instagram_url:
            logger.warning(f"❌ Invalid URL format: {instagram_url}")
            return jsonify({'error': 'Please provide a valid Instagram URL'}), 400
        
        # Download using yt-dlp (most reliable)
        if YTDLP_AVAILABLE:
            logger.info("✅ Using yt-dlp for download")
            result = download_with_ytdlp(instagram_url, format_id, client_ip)
            return result
        elif INSTAGRAPI_AVAILABLE:
            logger.info("✅ Using instagrapi for download")
            return download_with_instagrapi(instagram_url)
        else:
            logger.error("❌ No download service available!")
            return jsonify({'error': 'Download service not available. Please try again later.'}), 503
    
    except Exception as e:
        logger.error(f"❌ Error in download: {str(e)}", exc_info=True)
        return jsonify({'error': f'Error processing request: {str(e)}'}), 500


def download_with_ytdlp(url, format_id='best', client_ip=None):
    """Download Instagram content using yt-dlp"""
    try:
        logger.info(f"🔄 Processing with yt-dlp: {url} | Format: {format_id}")
        
        # Validate format_id
        if not format_id or format_id.strip() == '':
            format_id = 'best'
            logger.warning("⚠️ Empty format_id, using 'best'")
        
        # Enhanced yt-dlp options for Instagram with better timeout handling
        ydl_opts = {
            'format': format_id,  # Use the selected format
            'outtmpl': os.path.join(DOWNLOAD_FOLDER, '%(id)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'socket_timeout': 120,
            'http_headers': {
                'User-Agent': get_random_user_agent(),  # Rotate user agents to avoid detection
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Referer': 'https://www.instagram.com/',
            },
            'retries': 5,
            'fragment_retries': 5,
            'skip_unavailable_fragments': True,
            'ignoreerrors': False,  # Set to False to catch errors properly
            'ratelimit': 1024 * 1024,
            'no_part': True,
        }
        
        logger.info(f"📥 Starting download (format: {format_id})...")
        
        # Retry logic for broken connections
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    logger.info(f"📥 Downloading... (attempt {attempt + 1}/{max_retries})")
                    info = ydl.extract_info(url, download=True)
                    
                    # Validate info object
                    if info is None:
                        raise Exception("extract_info returned None - possible authentication issue")
                    
                    filename = ydl.prepare_filename(info)
                    
                    # Validate filename
                    if not filename:
                        raise Exception("prepare_filename returned empty string")
                    
                    logger.info(f"✅ Downloaded: {filename}")
                    
                    if os.path.exists(filename):
                        file_size = os.path.getsize(filename)
                        logger.info(f"📊 File size: {file_size / (1024 * 1024):.2f} MB")
                        
                        return jsonify({
                            'success': True,
                            'message': 'Download successful!',
                            'filename': os.path.basename(filename),
                            'file_size': f"{file_size / (1024 * 1024):.2f} MB"
                        })
                    else:
                        raise Exception(f"File not found after download: {filename}")
                        
            except Exception as e:
                error_str = str(e).lower()
                logger.warning(f"⚠️ Attempt {attempt + 1} failed: {str(e)}")
                
                # Check for specific errors that shouldn't retry
                if 'setdefault' in error_str or 'nonetype' in error_str:
                    logger.error(f"❌ NoneType error - likely invalid format: {str(e)}")
                    raise Exception(f"Invalid format or content not available. Error: {str(e)}")
                
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.info(f"⏳ Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ All {max_retries} attempts failed")
                    raise e
                    
    except Exception as e:
        logger.error(f"❌ yt-dlp error: {str(e)}", exc_info=True)
        
        # Check if it's a rate-limit error
        error_str = str(e).lower()
        if 'rate-limit' in error_str or 'rate limit' in error_str or 'not available' in error_str:
            logger.warning("🚫 Instagram rate-limiting detected")
            
            # Mark this IP as rate-limited
            if client_ip:
                mark_rate_limited(client_ip)
            
            return jsonify({
                'error': f'🚫 Instagram is blocking downloads from this IP. Rate-limit duration: 10 minutes. This is Instagram protecting against bots.',
                'rate_limited': True,
                'wait_seconds': RATE_LIMIT_COOLDOWN
            }), 429
        
        # Try fallback method
        logger.info("🔄 Trying alternative download method...")
        try:
            return download_with_direct_extraction(url, format_id, client_ip)
        except Exception as e2:
            logger.error(f"❌ Alternative method also failed: {str(e2)}", exc_info=True)
            error_msg = str(e).split('\n')[0] if str(e) else "Connection failed"
            
            # Provide more helpful error message
            if 'setdefault' in str(e).lower() or 'nonetype' in str(e).lower():
                error_msg = "Invalid format selected or content not accessible. Try selecting a different quality."
            
            return jsonify({'error': f'Failed to download: {error_msg}. Instagram may have rate-limited you. Please try again in a few minutes.'}), 400


def download_with_direct_extraction(url, format_id='best', client_ip=None):
    """Try alternative methods to extract Instagram media"""
    try:
        logger.info(f"🔄 Attempting direct extraction method... (format: {format_id})")
        
        # Validate format_id
        if not format_id or format_id.strip() == '':
            format_id = 'best'
        
        # Use a simpler approach with aggressive timeout handling
        ydl_opts = {
            'format': format_id,
            'outtmpl': os.path.join(DOWNLOAD_FOLDER, '%(id)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'socket_timeout': 120,
            'http_headers': {
                'User-Agent': get_random_user_agent(),  # Rotate user agents
            },
            'retries': 5,
            'fragment_retries': 5,
            'skip_unavailable_fragments': True,
            'ignoreerrors': False,
            'no_part': True,
            'ratelimit': 1024 * 1024,
        }
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    logger.info(f"📥 Extracting with alternative settings... (attempt {attempt + 1}/{max_retries})")
                    info = ydl.extract_info(url, download=True)
                    
                    # Validate info object
                    if info is None:
                        raise Exception("extract_info returned None")
                    
                    filename = ydl.prepare_filename(info)
                    
                    if not filename:
                        raise Exception("prepare_filename returned empty string")
                    
                    logger.info(f"✅ Alternative method succeeded: {filename}")
                    
                    if os.path.exists(filename):
                        file_size = os.path.getsize(filename)
                        logger.info(f"📊 File size: {file_size / (1024 * 1024):.2f} MB")
                        
                        return jsonify({
                            'success': True,
                            'message': 'Download successful!',
                            'filename': os.path.basename(filename),
                            'file_size': f"{file_size / (1024 * 1024):.2f} MB"
                        })
                        
            except Exception as e:
                error_str = str(e).lower()
                logger.warning(f"⚠️ Alternative attempt {attempt + 1} failed: {str(e)}")
                
                # Check for NoneType errors - don't retry these
                if 'setdefault' in error_str or 'nonetype' in error_str:
                    logger.error(f"❌ NoneType error - format issue, not retrying")
                    raise Exception(f"Invalid format or authentication issue")
                
                if attempt < max_retries - 1:
                    time.sleep(3)
                else:
                    raise e
                    
    except Exception as e:
        logger.error(f"❌ Direct extraction failed: {str(e)}", exc_info=True)
        raise


def download_with_instagrapi(url):
    """Download Instagram content using instagrapi"""
    try:
        # Extract media ID from URL
        client = Client()
        
        # Parse the URL to get media ID
        if 'instagram.com/p/' in url:
            media_id = url.split('/p/')[1].split('/')[0]
            media = client.media_info(media_id)
            
            if media.is_video:
                # Download video
                filename = f"{DOWNLOAD_FOLDER}/{media_id}_video.mp4"
                client.video_download(media_id, filename)
            else:
                # Download photo
                filename = f"{DOWNLOAD_FOLDER}/{media_id}_photo.jpg"
                client.photo_download(media_id, filename)
            
            file_size = os.path.getsize(filename)
            return jsonify({
                'success': True,
                'message': 'Download successful!',
                'filename': os.path.basename(filename),
                'file_size': f"{file_size / (1024 * 1024):.2f} MB"
            })
    except Exception as e:
        return jsonify({'error': f'Failed to download: {str(e)}'}), 400


@app.route('/api/file/<filename>', methods=['GET'])
def get_file(filename):
    """Serve downloaded files"""
    try:
        file_path = os.path.join(DOWNLOAD_FOLDER, filename)
        
        # Security check: ensure the file is in the downloads folder
        if not os.path.abspath(file_path).startswith(os.path.abspath(DOWNLOAD_FOLDER)):
            return jsonify({'error': 'Invalid file'}), 403
        
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True)
        else:
            return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/status', methods=['GET'])
def status():
    """Check service status"""
    return jsonify({
        'status': 'online',
        'yt_dlp_available': YTDLP_AVAILABLE,
        'instagrapi_available': INSTAGRAPI_AVAILABLE
    })


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)

