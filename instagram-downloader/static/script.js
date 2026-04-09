// Cache for history
let downloadHistory = JSON.parse(localStorage.getItem('downloadHistory')) || [];

// DOM Elements
const urlInput = document.getElementById('instagram-url');
const downloadBtn = document.getElementById('download-btn-main');
const downloadBtnPreview = document.getElementById('download-btn');
const messageDiv = document.getElementById('message');
const resultDiv = document.getElementById('result');
const filenameSpan = document.getElementById('filename');
const fileSizeSpan = document.getElementById('file-size');
const downloadLink = document.getElementById('download-link');
const historyList = document.getElementById('history-list');
const spinner = document.querySelector('.spinner');
const btnText = document.querySelector('.btn-text');

// Preview Elements
const previewSection = document.getElementById('preview-section');
const optionsSection = document.getElementById('options-section');
const previewThumbnail = document.getElementById('preview-thumbnail');
const previewTitle = document.getElementById('preview-title');
const previewDescription = document.getElementById('preview-description');
const previewType = document.getElementById('preview-type');
const previewDuration = document.getElementById('preview-duration');
const videoIcon = document.getElementById('video-icon');

// Event Listeners
downloadBtn.addEventListener('click', () => handleDownload('main'));
if (downloadBtnPreview) {
    downloadBtnPreview.addEventListener('click', () => handleDownload('preview'));
}
urlInput.addEventListener('change', handleUrlChange);
urlInput.addEventListener('blur', handleUrlChange);
urlInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        handleDownload('main');
    }
});

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    updateHistoryDisplay();
});

/**
 * Handle URL input change - fetch preview
 */
async function handleUrlChange() {
    const url = urlInput.value.trim();

    if (!url || !isValidInstagramUrl(url)) {
        previewSection.classList.add('hidden');
        return;
    }

    try {
        console.log('📸 Fetching preview for:', url);
        const response = await fetch('/api/preview', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ url: url }),
        });

        const data = await response.json();

        if (response.ok && data.success && data.preview) {
            displayPreview(data.preview);
        } else {
            previewSection.classList.add('hidden');
            console.warn('Could not fetch preview:', data.error);
        }
    } catch (error) {
        console.warn('Preview fetch error:', error);
        previewSection.classList.add('hidden');
    }
}

/**
 * Display preview information
 */
function displayPreview(preview) {
    // Show preview card, hide old options section
    previewSection.classList.remove('hidden');
    optionsSection.style.display = 'none';

    // Set thumbnail
    if (preview.thumbnail) {
        previewThumbnail.src = preview.thumbnail;
        previewThumbnail.style.display = 'block';

        // Show video icon if it's a video
        if (preview.is_video) {
            videoIcon.style.display = 'block';
        } else {
            videoIcon.style.display = 'none';
        }
    } else {
        previewThumbnail.style.display = 'none';
        videoIcon.style.display = 'none';
    }

    // Set title
    previewTitle.textContent = preview.title || 'Instagram Media';

    // Set description
    if (preview.description) {
        // Truncate long descriptions to 300 chars
        const desc = preview.description.length > 300
            ? preview.description.substring(0, 300) + '...'
            : preview.description;
        previewDescription.textContent = desc;
        previewDescription.style.display = 'block';
    } else {
        previewDescription.style.display = 'none';
    }

    // Set type
    if (preview.is_video) {
        previewType.textContent = '📹 Video';
        previewType.style.display = 'inline-block';
    } else {
        previewType.textContent = '📸 Photo';
        previewType.style.display = 'inline-block';
    }

    // Set duration
    if (preview.duration) {
        previewDuration.textContent = '⏱️ ' + preview.duration;
        previewDuration.style.display = 'inline-block';
    } else {
        previewDuration.style.display = 'none';
    }

    console.log('✅ Preview displayed:', preview);

    // Scroll to preview
    setTimeout(() => {
        previewSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 100);
}

/**
 * Handle download button click
 */
async function handleDownload(source = 'main') {
    const url = urlInput.value.trim();

    // Validation
    if (!url) {
        showMessage('Please enter an Instagram URL', 'error');
        return;
    }

    if (!isValidInstagramUrl(url)) {
        showMessage('Please enter a valid Instagram URL (instagram.com)', 'error');
        return;
    }

    // If preview is not showing and clicked from main button, fetch it first
    if (source === 'main' && previewSection.classList.contains('hidden')) {
        console.log('📸 Fetching preview before download...');
        try {
            const response = await fetch('/api/preview', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ url: url }),
            });

            const data = await response.json();

            if (response.ok && data.success && data.preview) {
                displayPreview(data.preview);
                showMessage('✅ Preview loaded! Now select options and click Download again.', 'success');
                return;
            }
        } catch (error) {
            console.warn('Preview fetch failed, continuing with download anyway', error);
        }
    }

    // Show loading state
    setLoadingState(true);
    hideMessage();
    resultDiv.classList.add('hidden');

    try {
        console.log('🔄 Starting download with best quality...');

        // Get selected content type
        const contentType = document.querySelector('input[name="content-type"]:checked').value;
        console.log('Content type selected:', contentType);

        const response = await fetch('/api/download', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                url: url,
                content_type: contentType
            }),
        });

        console.log('Response status:', response.status);
        const data = await response.json();
        console.log('Response data:', data);

        if (response.ok && data.success) {
            handleDownloadSuccess(data, url);
        } else {
            showMessage(data.error || 'Download failed', 'error');
            console.error('Download error:', data);
        }
    } catch (error) {
        console.error('Download error:', error);
        showMessage(`Connection error: ${error.message}. Please try again.`, 'error');
    } finally {
        setLoadingState(false);
    }
}


/**
 * Handle successful download
 */
function handleDownloadSuccess(data, url) {
    // Update result display
    filenameSpan.textContent = data.filename;
    fileSizeSpan.textContent = data.file_size;

    // Set download link
    downloadLink.href = `/api/file/${encodeURIComponent(data.filename)}`;
    downloadLink.download = data.filename;

    // Show result
    resultDiv.classList.remove('hidden');
    showMessage('✅ Download successful! Click the button below to save.', 'success');

    // Add to history
    const historyItem = {
        filename: data.filename,
        url: url,
        timestamp: new Date().toLocaleString(),
        size: data.file_size,
    };
    downloadHistory.unshift(historyItem);

    // Keep only last 10 items
    if (downloadHistory.length > 10) {
        downloadHistory.pop();
    }

    localStorage.setItem('downloadHistory', JSON.stringify(downloadHistory));
    updateHistoryDisplay();

    // Clear input and reset preview
    urlInput.value = '';
    previewSection.classList.add('hidden');
    optionsSection.style.display = 'block';
}

/**
 * Validate Instagram URL
 */
function isValidInstagramUrl(url) {
    try {
        const parsedUrl = new URL(url);
        return parsedUrl.hostname.includes('instagram.com');
    } catch {
        return url.includes('instagram.com');
    }
}

/**
 * Show message to user
 */
function showMessage(message, type = 'info') {
    messageDiv.textContent = message;
    messageDiv.className = `message ${type}`;
}

/**
 * Hide message
 */
function hideMessage() {
    messageDiv.classList.add('hidden');
}

/**
 * Set loading state
 */
function setLoadingState(isLoading) {
    downloadBtn.disabled = isLoading;
    if (downloadBtnPreview) {
        downloadBtnPreview.disabled = isLoading;
    }

    if (isLoading) {
        spinner.classList.remove('hidden');
        btnText.textContent = '⏳ Downloading...';
    } else {
        spinner.classList.add('hidden');
        btnText.textContent = '📥 Download';
    }
}

/**
 * Update history display
 */
function updateHistoryDisplay() {
    if (downloadHistory.length === 0) {
        historyList.innerHTML = '<p class="empty-state">No downloads yet</p>';
        return;
    }

    historyList.innerHTML = downloadHistory
        .map((item, index) => {
            const truncatedUrl = item.url.length > 40
                ? item.url.substring(0, 40) + '...'
                : item.url;

            return `
                <div class="history-item">
                    <div>
                        <strong>${item.filename}</strong><br>
                        <small>${item.timestamp}</small>
                    </div>
                </div>
            `;
        })
        .join('');
}

/**
 * Check service status
 */
async function checkServiceStatus() {
    try {
        const response = await fetch('/api/status');
        const data = await response.json();

        if (!data.yt_dlp_available && !data.instagrapi_available) {
            showMessage(
                '⚠️ Download service may not be fully available. Please check server logs.',
                'info'
            );
        }
    } catch (error) {
        console.error('Could not check service status:', error);
    }
}

// Optional: Add drag and drop support for URL
document.addEventListener('dragover', (e) => {
    e.preventDefault();
    urlInput.style.borderColor = 'var(--primary-color)';
});

document.addEventListener('dragleave', () => {
    urlInput.style.borderColor = 'var(--border-color)';
});

document.addEventListener('drop', (e) => {
    e.preventDefault();
    urlInput.style.borderColor = 'var(--border-color)';

    const text = e.dataTransfer.getData('text/plain');
    if (text.includes('instagram.com')) {
        urlInput.value = text;
        urlInput.focus();
    }
});

