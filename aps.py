import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import time

from config import Config

app = Flask(__name__, static_folder='../static')
CORS(app, resources={r"/*": {"origins": [
    "https://swingtradingwithme.blogspot.com",
    "https://aitradinglab.blogspot.com",
    "https://www.aitradinglab.in",
    "https://bansalshubham257.github.io",
    "http://localhost:63342"
]}}, supports_credentials=True)

# Worker service URL - change this to your actual worker URL in production
WORKER_URL = Config.WORKER_URL

@app.route('/api/<path:endpoint>', methods=['GET'])
def proxy_to_worker(endpoint):
    """Proxy API requests to the worker service"""
    try:
        response = requests.get(
            f"{WORKER_URL}/api/{endpoint}",
            params=request.args,
            timeout=10
        )
        return response.json(), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"error": f"Worker service error: {str(e)}"}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)))