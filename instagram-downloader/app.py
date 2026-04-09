from flask import Flask, render_template, request, jsonify, send_file, Response, stream_with_context, after_this_request
import os
import requests
from urllib.parse import urlparse
import json
from pathlib import Path
import logging
import sys
import time
import random
from collections import defaultdict
from threading import Lock
from queue import Queue, Empty
import threading

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-change-in-production')

# Always use /tmp — no files are permanently stored on disk.
# Files are deleted automatically the moment the browser finishes downloading them.
IS_PRODUCTION = os.environ.get('FLASK_ENV') == 'production'
DOWNLOAD_FOLDER       = '/tmp/igdl_downloads'
COOKIES_FILE          = '/tmp/instagram_cookies.json'
NETSCAPE_COOKIES_FILE = '/tmp/instagram_cookies.txt'
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Setup logging — file handler is optional (may fail on read-only filesystems)
log_handlers = [logging.StreamHandler(sys.stdout)]
try:
    log_handlers.append(logging.FileHandler('app.log'))
except (OSError, PermissionError):
    pass  # Skip file logging on production if not writable

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=log_handlers
)
logger = logging.getLogger(__name__)
logger.info("🚀 Starting Instagram Downloader - Multi-Method Version")

# Try to import yt-dlp
try:
    import yt_dlp
    YTDLP_AVAILABLE = True
    logger.info("✅ yt-dlp available")
except ImportError:
    YTDLP_AVAILABLE = False
    logger.warning("⚠️ yt-dlp not available")

# Try to import instagrapi
try:
    from instagrapi import Client
    INSTAGRAPI_AVAILABLE = True
    logger.info("✅ Instagrapi available")
except ImportError:
    INSTAGRAPI_AVAILABLE = False
    logger.warning("⚠️ Instagrapi not available")

# Try to import Selenium
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
    logger.info("✅ Selenium available")
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("⚠️ Selenium not available")

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
]

def get_random_user_agent():
    return random.choice(USER_AGENTS)

# ── Cookie helpers ──────────────────────────────────────────────────────────

def load_cookies():
    """Load saved Instagram cookies from JSON file.
    Returns dict like {'sessionid': '...', 'csrftoken': '...'} or {}."""
    try:
        if os.path.exists(COOKIES_FILE):
            with open(COOKIES_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"⚠️ Could not load cookies: {e}")
    return {}

def save_cookies(cookies: dict):
    """Persist cookies dict to JSON file and regenerate the Netscape txt for yt-dlp."""
    try:
        with open(COOKIES_FILE, 'w') as f:
            json.dump(cookies, f, indent=2)
        _write_netscape_cookies(cookies)
        logger.info(f"✅ Cookies saved ({list(cookies.keys())})")
    except Exception as e:
        logger.error(f"❌ Could not save cookies: {e}")

def _write_netscape_cookies(cookies: dict):
    """Write Netscape-format cookies file for yt-dlp."""
    expiry = int(time.time()) + 365 * 24 * 3600  # 1 year
    lines = ["# Netscape HTTP Cookie File", "# https://curl.se/docs/http-cookies.html", ""]
    for name, value in cookies.items():
        if value:
            lines.append(f".instagram.com\tTRUE\t/\tTRUE\t{expiry}\t{name}\t{value}")
    with open(NETSCAPE_COOKIES_FILE, 'w') as f:
        f.write('\n'.join(lines) + '\n')

def get_session_headers(extra: dict = None):
    """Build request headers with Instagram session cookies if available."""
    cookies = load_cookies()
    headers = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1',
        'x-ig-app-id': '936619743392459',
        'x-requested-with': 'XMLHttpRequest',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.instagram.com/',
        'Origin': 'https://www.instagram.com',
    }
    if cookies:
        cookie_str = '; '.join(f"{k}={v}" for k, v in cookies.items() if v)
        headers['Cookie'] = cookie_str
        if cookies.get('csrftoken'):
            headers['x-csrftoken'] = cookies['csrftoken']
    if extra:
        headers.update(extra)
    return headers

def cookies_are_set():
    cookies = load_cookies()
    return bool(cookies.get('sessionid'))

# ── URL type detection ──────────────────────────────────────────────────────
def detect_url_type(url):
    """
    Returns one of:
      YouTube  → 'yt_video' | 'yt_playlist' | 'yt_channel'
      Instagram→ 'profile'  | 'post'        | 'reel'
      Other    → 'unknown'
    """
    import re
    clean = url.split('?')[0].split('#')[0].rstrip('/')

    # ── YouTube ──────────────────────────────────────────────────────────
    if 'youtube.com' in url or 'youtu.be' in url:
        if 'list=' in url:                                      # playlist (may also have v=)
            return 'yt_playlist'
        if (re.search(r'youtube\.com/@[^/?#]+$', clean)        # @handle
                or '/channel/' in clean
                or re.search(r'youtube\.com/c/', clean)
                or re.search(r'youtube\.com/user/', clean)):
            return 'yt_channel'
        return 'yt_video'                                       # watch?v=, /shorts/, youtu.be

    # ── Instagram ────────────────────────────────────────────────────────
    if '/reel/' in clean or '/tv/' in clean:
        return 'reel'
    if '/p/' in clean:
        return 'post'
    if re.search(r'instagram\.com/([A-Za-z0-9_.]+)$', clean) and \
       not any(x in clean for x in ['/p/', '/reel/', '/tv/', '/stories/', '/explore/', '/accounts/']):
        return 'profile'

    return 'unknown'


@app.route('/')
def index():
    return render_template('index.html')


# ── /api/detect ──────────────────────────────────────────────────────────────
@app.route('/api/detect', methods=['POST'])
def detect():
    """Tell frontend what kind of URL was pasted."""
    url = (request.json or {}).get('url', '').strip()
    if not url or ('instagram.com' not in url and 'youtube.com' not in url and 'youtu.be' not in url):
        return jsonify({'error': 'Not a recognised Instagram or YouTube URL'}), 400
    return jsonify({'url_type': detect_url_type(url)})


# ── /api/set-cookie ──────────────────────────────────────────────────────────
@app.route('/api/set-cookie', methods=['POST'])
def set_cookie():
    """Save Instagram session cookies provided by the user."""
    data = request.json or {}
    sessionid = data.get('sessionid', '').strip()
    csrftoken  = data.get('csrftoken', '').strip()
    ds_user_id = data.get('ds_user_id', '').strip()

    if not sessionid:
        return jsonify({'error': 'sessionid is required'}), 400

    cookies = {'sessionid': sessionid}
    if csrftoken:
        cookies['csrftoken'] = csrftoken
    if ds_user_id:
        cookies['ds_user_id'] = ds_user_id

    save_cookies(cookies)
    return jsonify({'success': True, 'message': 'Cookies saved successfully'})


# ── /api/cookie-status ───────────────────────────────────────────────────────
@app.route('/api/cookie-status', methods=['GET'])
def cookie_status():
    cookies = load_cookies()
    has_session = bool(cookies.get('sessionid'))
    return jsonify({
        'has_cookies': has_session,
        'keys': list(cookies.keys()) if has_session else [],
    })


# ── /api/profile ─────────────────────────────────────────────────────────────
@app.route('/api/profile', methods=['POST'])
def get_profile_posts():
    """Fetch posts from an Instagram profile with pagination."""
    import re
    body      = request.json or {}
    url       = body.get('url', '').strip()
    max_posts = min(int(body.get('max_posts', 50)), 500)   # default 50, hard cap 500

    if not url or 'instagram.com' not in url:
        return jsonify({'error': 'Invalid Instagram URL'}), 400

    clean_url = url.split('?')[0].split('#')[0]
    username  = re.search(r'instagram\.com/([A-Za-z0-9_.]+)', clean_url)
    if not username:
        return jsonify({'error': 'Could not extract username'}), 400
    username = username.group(1)
    logger.info(f"👤 Fetching up to {max_posts} posts for @{username}")

    posts = []

    # ── Method 1: Mobile Feed API with next_max_id pagination (requires cookie) ──
    # Preferred when authenticated — supports 33 posts/page and proper pagination.
    if cookies_are_set():
        posts = _fetch_profile_graphql(username, max_posts)

    # ── Method 2: web_profile_info – always returns exactly 12, no working cursor ──
    # Used when unauthenticated OR as fallback if the feed API failed.
    # Instagram killed the old GraphQL cursor endpoint (query_hash returns 400).
    if not posts:
        posts = _fetch_profile_internal_api(username, max_posts)


    # ── Method 3: yt-dlp ──────────────────────────────────────────────────
    if not posts and YTDLP_AVAILABLE:
        try:
            logger.info("🔍 Trying yt-dlp for profile...")
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'skip_download': True,
                'extract_flat': True,
                'playlistend': max_posts,
                'socket_timeout': 30,
                'http_headers': {'User-Agent': get_random_user_agent()},
            }
            if os.path.exists(NETSCAPE_COOKIES_FILE):
                ydl_opts['cookiefile'] = NETSCAPE_COOKIES_FILE
                logger.info("🍪 Using saved cookies with yt-dlp")

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f'https://www.instagram.com/{username}/', download=False)
                if info and info.get('entries'):
                    for entry in info['entries']:
                        if not entry:
                            continue
                        post_url = entry.get('url') or entry.get('webpage_url') or f"https://www.instagram.com/p/{entry.get('id', '')}/"
                        posts.append({
                            'url':         post_url,
                            'thumbnail':   entry.get('thumbnail', ''),
                            'title':       entry.get('title', ''),
                            'description': entry.get('description', '') or entry.get('title', ''),
                            'is_video':    entry.get('ext') not in ('jpg', 'jpeg', 'png', 'webp'),
                            'duration':    _fmt_dur(entry.get('duration')),
                            'id':          entry.get('id', ''),
                        })
                    logger.info(f"✅ yt-dlp got {len(posts)} posts for @{username}")
        except Exception as e:
            logger.warning(f"⚠️ yt-dlp profile failed: {e}")

    # ── Method 4: HTML scraping ───────────────────────────────────────────
    if not posts:
        try:
            logger.info("🔍 Trying HTML scrape for profile...")
            headers = get_session_headers({'User-Agent': get_random_user_agent()})
            resp    = requests.get(f'https://www.instagram.com/{username}/', headers=headers, timeout=20)
            m = re.search(r'window\._sharedData\s*=\s*(\{.+?\});</script>', resp.text)
            if m:
                shared = json.loads(m.group(1))
                edges  = (shared.get('entry_data', {})
                                .get('ProfilePage', [{}])[0]
                                .get('graphql', {})
                                .get('user', {})
                                .get('edge_owner_to_timeline_media', {})
                                .get('edges', []))
                for e in edges[:max_posts]:
                    n = e.get('node', {})
                    thumb      = n.get('thumbnail_src') or n.get('display_url', '')
                    shortcode  = n.get('shortcode', '')
                    is_video   = n.get('is_video', False)
                    cap_edges  = n.get('edge_media_to_caption', {}).get('edges', [])
                    caption    = cap_edges[0]['node']['text'] if cap_edges else ''
                    posts.append({
                        'url':         f'https://www.instagram.com/p/{shortcode}/',
                        'thumbnail':   thumb,
                        'title':       caption[:80] if caption else 'Instagram Post',
                        'description': caption,
                        'is_video':    is_video,
                        'duration':    '',
                        'id':          shortcode,
                    })
                logger.info(f"✅ HTML scrape got {len(posts)} posts for @{username}")
        except Exception as e:
            logger.warning(f"⚠️ HTML scrape profile failed: {e}")

    if not posts:
        needs_cookie = not cookies_are_set()
        msg = (
            f'Could not fetch posts for @{username}. '
            + ('Instagram requires a session cookie. Use ⚙️ Settings to add yours.'
               if needs_cookie
               else 'Instagram is blocking requests. Your cookie may have expired — update it in ⚙️ Settings.')
        )
        return jsonify({'error': msg, 'needs_cookie': needs_cookie}), 400

    return jsonify({
        'success':  True,
        'username': username,
        'posts':    posts[:max_posts],
        'count':    len(posts[:max_posts]),
    })


def _fmt_dur(seconds):
    if not seconds:
        return ''
    s = int(seconds)
    return f"{s//60}:{s%60:02d}"


def _edge_to_post(e):
    """Convert a GraphQL edge node → post dict."""
    n         = e.get('node', {})
    shortcode = n.get('shortcode', '')
    is_video  = n.get('is_video', False)
    thumb     = n.get('thumbnail_src') or n.get('display_url', '')
    cap_edges = n.get('edge_media_to_caption', {}).get('edges', [])
    caption   = cap_edges[0]['node']['text'] if cap_edges else ''
    dur       = n.get('video_duration')
    return {
        'url':         f'https://www.instagram.com/p/{shortcode}/',
        'thumbnail':   thumb,
        'title':       caption[:80] if caption else 'Instagram Post',
        'description': caption,
        'is_video':    is_video,
        'duration':    _fmt_dur(dur),
        'id':          shortcode,
    }


def _item_to_post(item):
    """Convert a mobile-API item → post dict."""
    shortcode = item.get('code') or item.get('id', '')
    is_video  = item.get('media_type') == 2
    candidates = item.get('image_versions2', {}).get('candidates', [])
    thumb      = candidates[0]['url'] if candidates else ''
    cap_obj    = item.get('caption')
    caption    = cap_obj.get('text', '') if cap_obj else ''
    dur        = item.get('video_duration')
    return {
        'url':         f'https://www.instagram.com/p/{shortcode}/',
        'thumbnail':   thumb,
        'title':       caption[:80] if caption else 'Instagram Post',
        'description': caption,
        'is_video':    is_video,
        'duration':    _fmt_dur(dur),
        'id':          shortcode,
    }


def _fetch_profile_internal_api(username, max_posts=50):
    """
    Fetch the first page of posts via web_profile_info.
    Always returns exactly 12 posts (Instagram's page size).
    Cursor-based pagination via the old GraphQL query_hash endpoint
    was killed by Instagram in 2026 (returns 400 'Incorrect Query').
    For more posts, use _fetch_profile_graphql() which requires a session cookie.
    """
    posts = []
    try:
        logger.info(f"🔍 web_profile_info for @{username} (first 12, no pagination without auth)…")
        headers = get_session_headers()
        resp = requests.get(
            f'https://www.instagram.com/api/v1/users/web_profile_info/?username={username}',
            headers=headers, timeout=20,
        )
        logger.info(f"   web_profile_info status: {resp.status_code}")
        if resp.status_code != 200:
            return posts

        data     = resp.json()
        user     = data.get('data', {}).get('user', {})
        timeline = user.get('edge_owner_to_timeline_media', {})
        edges    = timeline.get('edges', [])
        pi       = timeline.get('page_info', {})

        posts = [_edge_to_post(e) for e in edges]
        logger.info(f"   Got {len(posts)} posts. has_next_page={pi.get('has_next_page')} "
                    f"(further pagination requires session cookie)")
    except Exception as e:
        logger.warning(f"⚠️ web_profile_info failed: {e}")
    return posts


def _fetch_profile_graphql(username, max_posts=50):
    """
    Fetch posts via the private mobile Feed API with next_max_id pagination.
    Requires a valid session cookie.
    """
    posts = []
    try:
        if not cookies_are_set():
            logger.info("⏭️  Feed API: no session cookie – skipping")
            return posts

        logger.info(f"🔍 Feed API (paginated) for @{username}, want {max_posts}…")
        headers = get_session_headers()

        # Resolve user_id
        resp = requests.get(
            f'https://www.instagram.com/api/v1/users/web_profile_info/?username={username}',
            headers=headers, timeout=20,
        )
        if resp.status_code != 200:
            logger.warning(f"⚠️ Feed API: user lookup returned {resp.status_code}")
            return posts

        user_id = resp.json().get('data', {}).get('user', {}).get('id')
        if not user_id:
            logger.warning("⚠️ Feed API: user_id not found")
            return posts

        mobile_ua   = ('Instagram 219.0.0.12.117 Android '
                       '(29/10; 420dpi; 1080x2400; Xiaomi; M2101K6G; garnet; qcom; en_US; 346055813)')
        next_max_id = None
        page        = 1

        while len(posts) < max_posts:
            feed_url = f'https://i.instagram.com/api/v1/feed/user/{user_id}/?count=33'
            if next_max_id:
                feed_url += f'&max_id={next_max_id}'

            feed_resp = requests.get(
                feed_url,
                headers=get_session_headers({'User-Agent': mobile_ua}),
                timeout=20,
            )
            logger.info(f"   Feed page {page} status: {feed_resp.status_code}")
            if feed_resp.status_code != 200:
                break

            feed_data   = feed_resp.json()
            items       = feed_data.get('items', [])
            next_max_id = feed_data.get('next_max_id')

            posts.extend(_item_to_post(i) for i in items)
            logger.info(f"   Feed page {page}: +{len(items)} posts (total {len(posts)})")

            if not items or not next_max_id:
                break
            page += 1
            time.sleep(0.4)

        logger.info(f"✅ Feed API: {len(posts)} posts fetched for @{username}")
    except Exception as e:
        logger.warning(f"⚠️ Feed API failed: {e}")
    return posts[:max_posts]


@app.route('/api/preview', methods=['POST'])
def get_preview():
    """Get preview information for Instagram URL"""
    try:
        data = request.json
        instagram_url = data.get('url', '').strip()
        
        logger.info(f"📸 Fetching preview for: {instagram_url}")

        if not instagram_url or (
            'instagram.com' not in instagram_url
            and 'youtube.com' not in instagram_url
            and 'youtu.be' not in instagram_url
        ):
            return jsonify({'error': 'Invalid URL'}), 400
        
        # Try to extract preview info
        preview_info = extract_preview_info(instagram_url)
        
        if preview_info:
            return jsonify({
                'success': True,
                'preview': preview_info
            })
        else:
            return jsonify({'error': 'Could not extract preview'}), 400
        
    except Exception as e:
        logger.error(f"❌ Error in get_preview: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

def extract_preview_info(url):
    """Extract preview information from Instagram URL using multiple methods"""
    import re

    preview_data = {}

    # ── Method 1: yt-dlp (most reliable, gets real caption + thumbnail) ──
    if YTDLP_AVAILABLE:
        try:
            logger.info("🔍 Extracting preview via yt-dlp...")
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'skip_download': True,
                'extract_flat': False,
                'socket_timeout': 20,
                'http_headers': {'User-Agent': get_random_user_agent()},
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if info:
                    preview_data['thumbnail']   = info.get('thumbnail') or (info.get('thumbnails') or [{}])[-1].get('url', '')
                    preview_data['title']       = info.get('title', '')
                    preview_data['description'] = info.get('description', '')   # keep separate from title
                    preview_data['is_video']    = info.get('ext') not in ('jpg', 'png', 'jpeg', 'webp')
                    preview_data['type']        = 'video' if preview_data['is_video'] else 'photo'

                    # Duration
                    dur = info.get('duration')
                    if dur:
                        preview_data['duration'] = f"{int(dur)//60}:{int(dur)%60:02d}"

                    logger.info(f"✅ yt-dlp preview: thumbnail={'yes' if preview_data.get('thumbnail') else 'no'}, desc_len={len(preview_data.get('description',''))}")
                    return preview_data
        except Exception as e:
            logger.warning(f"⚠️ yt-dlp preview failed: {e}")

    # ── Method 2: HTML scraping (og: meta tags) ──
    try:
        logger.info("🔍 Extracting preview via HTML scraping...")
        headers = {'User-Agent': get_random_user_agent()}
        resp = requests.get(url, headers=headers, timeout=20)

        if resp.status_code == 200:
            html = resp.text

            def _og(prop):
                m = re.search(rf'og:{prop}"[^>]*content=["\']([^"\']+)["\']', html) or \
                    re.search(rf'content=["\']([^"\']+)["\'][^>]*og:{prop}', html)
                return m.group(1) if m else ''

            thumbnail   = _og('image')
            title       = _og('title')
            description = _og('description')
            is_video    = bool(re.search(r'og:video', html))

            if thumbnail or description:
                preview_data['thumbnail']   = thumbnail
                preview_data['title']       = title
                preview_data['description'] = description   # keep separate; frontend will show both
                preview_data['is_video']    = is_video
                preview_data['type']        = 'video' if is_video else 'photo'

                # Duration from JSON blob
                dur_m = re.search(r'"video_duration"\s*:\s*(\d+(?:\.\d+)?)', html)
                if dur_m:
                    dur = float(dur_m.group(1))
                    preview_data['duration'] = f"{int(dur)//60}:{int(dur)%60:02d}"

                logger.info(f"✅ HTML scrape preview: thumbnail={'yes' if thumbnail else 'no'}, desc_len={len(description)}")
                return preview_data

    except Exception as e:
        logger.warning(f"⚠️ HTML scrape preview failed: {e}")

    logger.warning("⚠️ All preview methods failed")
    return None


@app.route('/api/download', methods=['POST'])
def download():
    try:
        data         = request.json or {}
        url          = data.get('url', '').strip()
        content_type = data.get('content_type', 'both')   # 'video' | 'audio' | 'both'
        quality      = data.get('quality', 'best')         # YouTube quality selector

        logger.info(f"📥 Download: {url} | content={content_type} | quality={quality}")

        if not url:
            return jsonify({'error': 'URL required'}), 400

        # ── YouTube ──
        if 'youtube.com' in url or 'youtu.be' in url:
            if not YTDLP_AVAILABLE:
                return jsonify({'error': 'yt-dlp not installed'}), 500
            result = download_youtube(url, quality, content_type)
            if result and result.status_code == 200:
                return result
            return jsonify({'error': 'YouTube download failed. Try a different quality or check the URL.'}), 400

        # ── Instagram ──
        if 'instagram.com' not in url:
            return jsonify({'error': 'Unsupported URL. Paste an Instagram or YouTube link.'}), 400

        return try_download_methods(url, 'best', content_type)

    except Exception as e:
        logger.error(f"❌ Download error: {e}", exc_info=True)
        return jsonify({'error': f'Download failed: {str(e)}'}), 500

def try_download_methods(url, format_id, content_type='both'):
    """Try multiple download methods in order of reliability"""

    methods = []

    # Method 1: yt-dlp for videos; automatically falls through to photo handler on "no video"
    if YTDLP_AVAILABLE:
        methods.append(('yt-dlp', download_with_ytdlp, [url, format_id, content_type]))

    # Method 2: Dedicated Instagram photo downloader (Instagram API + CDN direct)
    methods.append(('Instagram Photo', download_instagram_photo, [url]))

    # Method 3: Instagrapi
    if INSTAGRAPI_AVAILABLE:
        methods.append(('Instagrapi', download_with_instagrapi, [url, content_type]))

    # Method 4: Direct HTTP extraction
    methods.append(('Direct HTTP', download_direct_http, [url, content_type]))

    for method_name, method_func, args in methods:
        try:
            logger.info(f"🔄 Trying: {method_name}")
            result = method_func(*args)
            if result and result.status_code == 200:
                logger.info(f"✅ Success with {method_name}")
                return result
        except Exception as e:
            logger.warning(f"⚠️ {method_name} failed: {str(e)}")
            continue

    return jsonify({
        'error': 'All download methods failed. Instagram may be blocking this content. Try again in a few minutes.'
    }), 400


# ── YouTube ──────────────────────────────────────────────────────────────────

YT_FORMATS = {
    'best':  'bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best',
    '1080p': 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]',
    '720p':  'bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720]',
    '480p':  'bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480]',
    '360p':  'bestvideo[height<=360][ext=mp4]+bestaudio[ext=m4a]/best[height<=360]',
    'audio': 'bestaudio[ext=m4a]/bestaudio',
}


def download_youtube(url, quality='best', content_type='both'):
    """Download a YouTube video/audio with the requested quality using yt-dlp."""
    try:
        is_audio = (content_type == 'audio' or quality == 'audio')
        fmt      = YT_FORMATS['audio'] if is_audio else YT_FORMATS.get(quality, YT_FORMATS['best'])
        logger.info(f"▶️ YouTube download: quality={quality} | fmt={fmt[:50]}")

        ydl_opts = {
            'format':         fmt,
            'outtmpl':        os.path.join(DOWNLOAD_FOLDER, '%(uploader)s_%(id)s.%(ext)s'),
            'quiet':          True,
            'no_warnings':    True,
            'socket_timeout': 120,
            'retries':        3,
        }
        if not is_audio:
            ydl_opts['merge_output_format'] = 'mp4'
        else:
            ydl_opts['postprocessors'] = [{
                'key':              'FFmpegExtractAudio',
                'preferredcodec':   'mp3',
                'preferredquality': '192',
            }]

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info:
                filename = ydl.prepare_filename(info)
                # Extension may change after merge/conversion – scan alternatives
                if not os.path.exists(filename):
                    base = os.path.splitext(filename)[0]
                    for ext in ('mp4', 'mkv', 'webm', 'mp3', 'm4a', 'ogg'):
                        candidate = f"{base}.{ext}"
                        if os.path.exists(candidate):
                            filename = candidate
                            break
                if os.path.exists(filename):
                    file_size = os.path.getsize(filename)
                    logger.info(f"✅ YouTube: {os.path.basename(filename)} ({file_size/(1024*1024):.2f} MB)")
                    return jsonify({
                        'success':   True,
                        'filename':  os.path.basename(filename),
                        'file_size': f"{file_size/(1024*1024):.2f} MB",
                    })
        return None
    except Exception as e:
        logger.warning(f"⚠️ YouTube download failed: {e}")
        return None


@app.route('/api/youtube/playlist', methods=['POST'])
def get_youtube_playlist():
    """Fetch video list from a YouTube playlist or channel."""
    body       = request.json or {}
    url        = body.get('url', '').strip()
    max_videos = min(int(body.get('max_posts', 50)), 200)

    if not ('youtube.com' in url or 'youtu.be' in url):
        return jsonify({'error': 'Not a YouTube URL'}), 400
    if not YTDLP_AVAILABLE:
        return jsonify({'error': 'yt-dlp not available on this server'}), 500

    try:
        logger.info(f"📋 YouTube playlist/channel: {url}, max={max_videos}")
        ydl_opts = {
            'quiet':          True,
            'no_warnings':    True,
            'extract_flat':   'in_playlist',
            'playlistend':    max_videos,
            'socket_timeout': 30,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        if not info:
            return jsonify({'error': 'Could not fetch YouTube content'}), 400

        entries = info.get('entries') or []
        videos  = []
        for entry in entries:
            if not entry:
                continue
            vid_id    = entry.get('id', '')
            if not vid_id:
                continue
            vid_url   = f'https://www.youtube.com/watch?v={vid_id}'
            # Prefer maxresdefault (1280×720); hqdefault (480×360) always exists as fallback
            thumb = (entry.get('thumbnail')
                     or f'https://i.ytimg.com/vi/{vid_id}/maxresdefault.jpg')
            videos.append({
                'id':          vid_id,
                'url':         vid_url,
                'title':       entry.get('title', 'Unknown'),
                'description': entry.get('description', ''),
                'thumbnail':   thumb,
                'duration':    _fmt_dur(entry.get('duration')),
                'is_video':    True,
                'platform':    'youtube',
                'uploader':    entry.get('uploader', '') or entry.get('channel', ''),
            })

        title    = info.get('title', 'YouTube')
        uploader = info.get('uploader', '') or info.get('channel', '')
        logger.info(f"✅ YouTube: {len(videos)} videos from '{title}'")
        return jsonify({
            'success':  True,
            'title':    title,
            'uploader': uploader,
            'count':    len(videos),
            'videos':   videos,
        })
    except Exception as e:
        logger.error(f"❌ YouTube playlist error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400


# ── Shortcode helpers ────────────────────────────────────────────────────────

_SHORTCODE_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'

def shortcode_to_media_pk(shortcode):
    """Convert an Instagram shortcode (e.g. 'DWp3ZD9FAmR') to its numeric media ID."""
    media_id = 0
    for char in shortcode:
        media_id = media_id * 64 + _SHORTCODE_ALPHABET.index(char)
    return media_id

def _extract_shortcode(url):
    """Return the shortcode portion of an Instagram post/reel/tv URL."""
    for seg in ('/p/', '/reel/', '/tv/'):
        if seg in url:
            return url.split(seg)[1].split('/')[0].split('?')[0]
    return None

def _download_cdn_image(img_url, shortcode):
    """Fetch an image from Instagram CDN with proper Referer + session cookie headers."""
    try:
        cookies = load_cookies()
        headers = {
            'User-Agent': get_random_user_agent(),
            'Referer':    'https://www.instagram.com/',
            'Accept':     'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        if cookies:
            headers['Cookie'] = '; '.join(f"{k}={v}" for k, v in cookies.items() if v)

        resp = requests.get(img_url, headers=headers, timeout=60, stream=True)
        if resp.status_code != 200:
            logger.warning(f"⚠️ CDN returned {resp.status_code}")
            return None

        ct  = resp.headers.get('Content-Type', 'image/jpeg')
        ext = 'png' if 'png' in ct else 'jpg'
        filename = os.path.join(DOWNLOAD_FOLDER, f"{shortcode}_{int(time.time())}.{ext}")

        with open(filename, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            file_size = os.path.getsize(filename)
            logger.info(f"✅ Photo saved: {os.path.basename(filename)} ({file_size/1024:.1f} KB)")
            return jsonify({
                'success':   True,
                'filename':  os.path.basename(filename),
                'file_size': f"{file_size / (1024*1024):.2f} MB",
            })
        return None
    except Exception as e:
        logger.warning(f"⚠️ CDN image download failed: {e}")
        return None


# ── Public helpers (no auth needed) ─────────────────────────────────────────

def _fetch_image_from_embed(shortcode):
    """
    Hit Instagram's embed endpoint – publicly accessible, no cookies needed.
    Returns a CDN image URL string or None.
    """
    import re
    try:
        embed_url = f'https://www.instagram.com/p/{shortcode}/embed/captioned/'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/124.0.0.0 Safari/537.36',
            'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer':         'https://www.instagram.com/',
        }
        resp = requests.get(embed_url, headers=headers, timeout=20)
        logger.info(f"   Embed endpoint status: {resp.status_code}")
        if resp.status_code != 200:
            return None
        html = resp.text
        # Patterns from most- to least-specific
        patterns = [
            # Dedicated embed image class
            r'<img[^>]+class="[^"]*EmbeddedMediaImage[^"]*"[^>]+src="([^"]+)"',
            r'<img[^>]+src="([^"]+)"[^>]+class="[^"]*EmbeddedMediaImage[^"]*"',
            # Any scontent/cdninstagram image
            r'<img[^>]+src="(https://[^"]*(?:scontent|cdninstagram)[^"]*\.(?:jpg|png|jpeg)[^"]*)"',
            # JSON blob inside embed HTML
            r'"display_url"\s*:\s*"(https://[^"]+)"',
            r'"thumbnail_src"\s*:\s*"(https://[^"]+)"',
            # CSS background
            r'background-image:\s*url\([\'"]?(https://[^\'")]+)[\'"]?\)',
        ]
        for pat in patterns:
            m = re.search(pat, html)
            if m:
                img_url = _unescape_url(m.group(1))
                logger.info(f"   Embed → image URL found")
                return img_url
        logger.warning("   Embed: page fetched but no image URL matched")
    except Exception as e:
        logger.warning(f"⚠️ Embed fetch failed: {e}")
    return None


def _fetch_oembed_thumbnail(shortcode):
    """
    Use Instagram's public oEmbed API – returns a thumbnail_url (lower-res but reliable).
    No authentication required.
    """
    try:
        oembed_url = (f'https://www.instagram.com/api/v1/oembed/'
                      f'?url=https://www.instagram.com/p/{shortcode}/&hidecaption=0')
        headers = {
            'User-Agent': get_random_user_agent(),
            'Referer':    'https://www.instagram.com/',
        }
        resp = requests.get(oembed_url, headers=headers, timeout=20)
        logger.info(f"   oEmbed status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            thumb = data.get('thumbnail_url')
            if thumb:
                logger.info("   oEmbed → thumbnail URL found")
                return thumb
    except Exception as e:
        logger.warning(f"⚠️ oEmbed fetch failed: {e}")
    return None


def _unescape_url(url):
    """Decode HTML entities and JSON escapes in a URL string."""
    return (url
            .replace('&amp;', '&')
            .replace('\\u0026', '&')
            .replace('\\/', '/'))


# ── Dedicated photo downloader ───────────────────────────────────────────────

def download_instagram_photo(url):
    """
    Download a photo post.  Methods tried in order:
      A) Instagram internal /media/{pk}/info/ API  (only if session cookie is set)
      B) Instagram embed endpoint                  (public – no auth needed)
      C) oEmbed thumbnail API                      (public – no auth, lower-res)
      D) Page scrape (display_url / og:image)      (may work for some posts)
    """
    import re
    shortcode = _extract_shortcode(url)
    if not shortcode:
        return None

    logger.info(f"📸 Photo downloader for shortcode: {shortcode}")

    # ── Method A: internal API (requires session cookie) ─────────────────
    if cookies_are_set():
        try:
            media_pk = shortcode_to_media_pk(shortcode)
            api_resp = requests.get(
                f'https://www.instagram.com/api/v1/media/{media_pk}/info/',
                headers=get_session_headers(), timeout=20,
            )
            logger.info(f"   Internal API status: {api_resp.status_code}")
            if api_resp.status_code == 200 and api_resp.text.strip():
                item      = (api_resp.json().get('items') or [{}])[0]
                media_type = item.get('media_type')  # 1=photo, 2=video, 8=album
                if media_type == 2:
                    logger.info("⏭️  Photo downloader: media is a video – skipping")
                    return None
                candidates = item.get('image_versions2', {}).get('candidates', [])
                if candidates:
                    candidates.sort(key=lambda x: x.get('width', 0) * x.get('height', 0), reverse=True)
                    img_url = candidates[0]['url']
                    logger.info(f"   Internal API → {candidates[0].get('width')}×{candidates[0].get('height')} image")
                    result = _download_cdn_image(img_url, shortcode)
                    if result:
                        return result
        except Exception as e:
            logger.warning(f"⚠️ Photo internal API failed: {e}")
    else:
        logger.info("   Internal API: no session cookie – skipping")

    # ── Method B: public embed endpoint ─────────────────────────────────
    try:
        img_url = _fetch_image_from_embed(shortcode)
        if img_url:
            result = _download_cdn_image(img_url, shortcode)
            if result:
                return result
    except Exception as e:
        logger.warning(f"⚠️ Embed method failed: {e}")

    # ── Method C: public oEmbed thumbnail ───────────────────────────────
    try:
        img_url = _fetch_oembed_thumbnail(shortcode)
        if img_url:
            result = _download_cdn_image(img_url, shortcode)
            if result:
                return result
    except Exception as e:
        logger.warning(f"⚠️ oEmbed method failed: {e}")

    # ── Method D: post page scrape ───────────────────────────────────────
    try:
        page_resp = requests.get(
            url,
            headers=get_session_headers({'User-Agent': get_random_user_agent()}),
            timeout=20,
        )
        logger.info(f"   Page scrape status: {page_resp.status_code}")
        if page_resp.status_code == 200:
            html = page_resp.text
            patterns = [
                r'"display_url"\s*:\s*"(https://[^"]+)"',
                r'"thumbnail_src"\s*:\s*"(https://[^"]+)"',
                r'<meta property="og:image" content="([^"]+)"',
                r'<meta content="([^"]+)" property="og:image"',
                r'<img[^>]+src="(https://[^"]*(?:scontent|cdninstagram)[^"]+\.(?:jpg|png))[^"]*"',
            ]
            matched = False
            for pat in patterns:
                m = re.search(pat, html)
                if m:
                    img_url = _unescape_url(m.group(1))
                    logger.info(f"   Page scrape → image URL found (pattern: {pat[:40]}…)")
                    result = _download_cdn_image(img_url, shortcode)
                    if result:
                        return result
                    matched = True  # pattern matched but CDN download failed
            if not matched:
                logger.warning("   Page scrape: no image URL patterns matched the HTML")
    except Exception as e:
        logger.warning(f"⚠️ Photo page scrape failed: {e}")

    logger.warning(f"⚠️ All photo download methods exhausted for {shortcode}")
    return None


# ── Video downloaders ────────────────────────────────────────────────────────

def download_with_instagrapi(url, content_type='both'):
    """Download using Instagrapi – uses media_pk_from_code() for proper shortcode→ID."""
    try:
        logger.info(f"📱 Downloading with Instagrapi (content: {content_type})...")
        shortcode = _extract_shortcode(url)
        if not shortcode:
            raise Exception("Could not extract shortcode from URL")

        client = Client()

        # Convert shortcode → numeric media PK (fixes 'invalid literal for int()' error)
        try:
            media_pk = client.media_pk_from_code(shortcode)
        except Exception:
            media_pk = shortcode_to_media_pk(shortcode)

        media = client.media_info(media_pk)

        if media.is_video:
            logger.info(f"📹 Video detected: {shortcode}")
            if content_type == 'audio':
                temp_video = os.path.join(DOWNLOAD_FOLDER, f"{shortcode}_temp.mp4")
                filename   = os.path.join(DOWNLOAD_FOLDER, f"{shortcode}_audio.mp3")
                client.video_download(media_pk, temp_video)
                try:
                    import subprocess
                    subprocess.run(
                        ['ffmpeg', '-i', temp_video, '-q:a', '0', '-map', 'a', filename, '-y'],
                        capture_output=True, timeout=300,
                    )
                    if os.path.exists(temp_video):
                        os.remove(temp_video)
                except Exception:
                    filename = os.path.join(DOWNLOAD_FOLDER, f"{shortcode}.mp4")
                    if os.path.exists(temp_video):
                        os.rename(temp_video, filename)
            else:
                filename = os.path.join(DOWNLOAD_FOLDER, f"{shortcode}.mp4")
                client.video_download(media_pk, filename)
        else:
            logger.info(f"📸 Photo detected: {shortcode}")
            filename = os.path.join(DOWNLOAD_FOLDER, f"{shortcode}.jpg")
            client.photo_download(media_pk, filename)

        if os.path.exists(filename):
            file_size = os.path.getsize(filename)
            logger.info(f"✅ Downloaded: {os.path.basename(filename)} ({file_size/(1024*1024):.2f} MB)")
            return jsonify({
                'success':   True,
                'filename':  os.path.basename(filename),
                'file_size': f"{file_size/(1024*1024):.2f} MB",
            })
        return None

    except Exception as e:
        logger.warning(f"⚠️ Instagrapi failed: {str(e)}")
        return None


def download_with_ytdlp(url, format_id='best', content_type='both'):
    """Download using yt-dlp. On 'no video' error, delegates to download_instagram_photo()."""
    try:
        logger.info(f"🔄 Downloading with yt-dlp (content: {content_type})...")

        if content_type == 'video':
            format_str = 'bestvideo[ext=mp4]/best[ext=mp4]'
        elif content_type == 'audio':
            format_str = 'bestaudio[ext=m4a]/bestaudio'
        else:
            format_str = format_id

        ydl_opts = {
            'format':       format_str,
            'outtmpl':      os.path.join(DOWNLOAD_FOLDER, '%(id)s.%(ext)s'),
            'quiet':        True,
            'no_warnings':  True,
            'socket_timeout': 90,
            'http_headers': {
                'User-Agent':      get_random_user_agent(),
                'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            },
            'retries': 2,
            'skip_unavailable_fragments': True,
        }
        if os.path.exists(NETSCAPE_COOKIES_FILE):
            ydl_opts['cookiefile'] = NETSCAPE_COOKIES_FILE

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info:
                filename = ydl.prepare_filename(info)
                # yt-dlp may change extension; scan for actual file
                if not os.path.exists(filename):
                    base = os.path.splitext(filename)[0]
                    for ext in ('mp4', 'jpg', 'jpeg', 'png', 'webp', 'mkv', 'mp3', 'm4a'):
                        candidate = f"{base}.{ext}"
                        if os.path.exists(candidate):
                            filename = candidate
                            break
                if os.path.exists(filename):
                    file_size = os.path.getsize(filename)
                    logger.info(f"✅ Downloaded: {os.path.basename(filename)} ({file_size/(1024*1024):.2f} MB)")
                    return jsonify({
                        'success':   True,
                        'filename':  os.path.basename(filename),
                        'file_size': f"{file_size/(1024*1024):.2f} MB",
                    })
        return None

    except Exception as e:
        err = str(e)
        # yt-dlp raises this for photo posts – hand off to the dedicated photo handler
        if 'no video' in err.lower():
            logger.info("📸 yt-dlp: no video in post – delegating to photo downloader")
            return download_instagram_photo(url)
        logger.warning(f"⚠️ yt-dlp failed: {err}")
        return None


def download_direct_http(url, content_type='both'):
    """Last-resort: scrape og:video / og:image from the post page and download directly."""
    import re
    try:
        logger.info(f"🌐 Trying direct HTTP download (content: {content_type})...")
        headers  = get_session_headers({'User-Agent': get_random_user_agent()})
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code != 200:
            logger.warning(f"⚠️ Direct HTTP: page returned {response.status_code}")
            return None

        logger.info("✅ Page fetched successfully")
        html = response.text

        # ── Video ──
        if content_type != 'audio':
            for pat in [
                r'<meta property="og:video" content="([^"]+)"',
                r'<meta content="([^"]+)" property="og:video"',
                r'"video_url"\s*:\s*"(https://[^"]+)"',
            ]:
                m = re.search(pat, html)
                if m:
                    video_url = _unescape_url(m.group(1))
                    logger.info("📹 Video URL found via Direct HTTP")
                    return _download_raw_url(video_url, 'mp4')

        # ── Photo ──
        for pat in [
            r'"display_url"\s*:\s*"(https://[^"]+)"',
            r'"thumbnail_src"\s*:\s*"(https://[^"]+)"',
            r'<meta property="og:image" content="([^"]+)"',
            r'<meta content="([^"]+)" property="og:image"',
            r'<img[^>]+src="(https://[^"]*(?:scontent|cdninstagram)[^"]+\.(?:jpg|png))[^"]*"',
        ]:
            m = re.search(pat, html)
            if m:
                img_url   = _unescape_url(m.group(1))
                shortcode = _extract_shortcode(url) or f"img_{int(time.time())}"
                logger.info("📸 Image URL found via Direct HTTP")
                return _download_cdn_image(img_url, shortcode)

        logger.warning("⚠️ Direct HTTP: no video or image URL patterns matched the HTML")
        return None

    except Exception as e:
        logger.warning(f"⚠️ Direct HTTP failed: {str(e)}")
        return None


def _download_raw_url(media_url, ext):
    """Generic URL → file download with Instagram Referer headers."""
    try:
        cookies = load_cookies()
        headers = {
            'User-Agent': get_random_user_agent(),
            'Referer':    'https://www.instagram.com/',
            'Accept':     '*/*',
        }
        if cookies:
            headers['Cookie'] = '; '.join(f"{k}={v}" for k, v in cookies.items() if v)

        resp = requests.get(media_url, headers=headers, timeout=60, stream=True)
        if resp.status_code == 200:
            filename = os.path.join(DOWNLOAD_FOLDER, f"media_{int(time.time())}.{ext}")
            with open(filename, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                file_size = os.path.getsize(filename)
                logger.info(f"✅ Raw download: {os.path.basename(filename)} ({file_size/(1024*1024):.2f} MB)")
                return jsonify({
                    'success':   True,
                    'filename':  os.path.basename(filename),
                    'file_size': f"{file_size/(1024*1024):.2f} MB",
                })
        return None
    except Exception as e:
        logger.warning(f"⚠️ Raw URL download failed: {e}")
        return None


# keep old name as alias so nothing else breaks
def download_media_url(media_url, ext):
    return _download_raw_url(media_url, ext)


@app.route('/api/thumbnail', methods=['GET'])
def proxy_thumbnail():
    """
    Proxy Instagram CDN thumbnail images to the browser.
    Instagram CDN blocks direct browser requests (wrong Referer / CORS).
    Usage: /api/thumbnail?url=<encoded_cdn_url>
    """
    from flask import Response
    from urllib.parse import unquote
    img_url = request.args.get('url', '').strip()
    if not img_url:
        return jsonify({'error': 'url param required'}), 400
    # Only allow Instagram / Facebook / YouTube CDN domains
    allowed = ('instagram.com', 'cdninstagram.com', 'fbcdn.net', 'fbsbx.com',
               'ytimg.com', 'ggpht.com', 'googleusercontent.com')
    if not any(d in img_url for d in allowed):
        return jsonify({'error': 'Disallowed domain'}), 403
    try:
        headers = {
            'User-Agent': get_random_user_agent(),
            'Referer':    'https://www.instagram.com/',
            'Accept':     'image/webp,image/apng,image/*,*/*;q=0.8',
        }
        # Forward session cookie to CDN if we have one (needed for some private content)
        cookies = load_cookies()
        if cookies:
            headers['Cookie'] = '; '.join(f"{k}={v}" for k, v in cookies.items() if v)
        resp = requests.get(img_url, headers=headers, timeout=15, stream=True)
        if resp.status_code != 200:
            return jsonify({'error': f'CDN returned {resp.status_code}'}), 502
        content_type = resp.headers.get('Content-Type', 'image/jpeg')
        return Response(
            resp.iter_content(chunk_size=8192),
            status=200,
            content_type=content_type,
            headers={
                'Cache-Control': 'public, max-age=3600',
            }
        )
    except Exception as e:
        logger.warning(f"⚠️ Thumbnail proxy failed: {e}")
        return jsonify({'error': str(e)}), 502


@app.route('/api/file/<filename>', methods=['GET'])
def get_file(filename):
    try:
        file_path = os.path.join(DOWNLOAD_FOLDER, filename)

        # Path traversal guard
        if not os.path.abspath(file_path).startswith(os.path.abspath(DOWNLOAD_FOLDER)):
            return jsonify({'error': 'Invalid file path'}), 403

        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404

        # Delete the temp file from /tmp as soon as the response is sent
        @after_this_request
        def _cleanup(response):
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"🗑️ Cleaned up temp file: {filename}")
            except Exception as e:
                logger.warning(f"⚠️ Could not delete {filename}: {e}")
            return response

        return send_file(file_path, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/status', methods=['GET'])
def status():
    return jsonify({
        'status': 'online',
        'yt_dlp': YTDLP_AVAILABLE,
        'instagrapi': INSTAGRAPI_AVAILABLE,
        'selenium': SELENIUM_AVAILABLE,
        'methods': [m for m in ['Instagrapi', 'yt-dlp', 'Direct HTTP'] if (m == 'Instagrapi' and INSTAGRAPI_AVAILABLE) or (m == 'yt-dlp' and YTDLP_AVAILABLE) or m == 'Direct HTTP']
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    debug = not IS_PRODUCTION
    app.run(debug=debug, host='0.0.0.0', port=port)

