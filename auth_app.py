import os
import webbrowser
import logging
from flask import Flask, redirect, request
import requests
from dotenv import load_dotenv
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Load environment variables
load_dotenv()

API_KEY = os.getenv("UPSTOX_API_KEY")
API_SECRET = os.getenv("UPSTOX_API_SECRET")
REDIRECT_URI = os.getenv("UPSTOX_REDIRECT_URI")

app = Flask(__name__)


@app.route('/')
def login():
    auth_url = (
        "https://api.upstox.com/v2/login/authorization/dialog"
        f"?client_id={API_KEY}"
        f"&redirect_uri={REDIRECT_URI}"
        "&response_type=code"
        # "&scope=offline_access"  # Excluded for now
    )
    logging.info("Redirecting user to Upstox OAuth login page")
    return redirect(auth_url)


@app.route('/callback')
def callback():
    code = request.args.get('code')
    if not code:
        logging.error("No authorization code received in callback")
        return "❌ No authorization code received. Please retry."

    logging.info(f"Authorization code received: {code}")

    payload = {
        "code": code,
        "client_id": API_KEY,
        "client_secret": API_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    token_url = "https://api.upstox.com/v2/login/authorization/token"

    # --- Exchange code for token ---
    try:
        response = requests.post(token_url, data=payload, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Request failed during token exchange: {e}")
        return f"❌ Request failed: {e}"

    token_data = response.json()
    logging.info(f"Token response received: {token_data}")

    access_token = token_data.get("access_token")
    if not access_token:
        logging.error(f"Invalid token response: {token_data}")
        return f"❌ Invalid token response: {token_data}"

    # --- Save token files (only file I/O inside try) ---
    token_path = os.path.abspath("token.txt")
    refresh_path = os.path.abspath("refresh_token.txt")

    try:
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(access_token)

        with open(refresh_path, "w", encoding="utf-8") as f:
            f.write("")  # currently unused
    except OSError as e:
        # This really means disk / permission / path issue
        logging.error("Failed to save token files on disk", exc_info=True)
        return f"❌ Failed to save token files on disk: {repr(e)}"

    # Logging kept OUTSIDE the try, so logging errors don't masquerade as file errors
    logging.info("Access token saved successfully at %s", token_path)
    return "✅ Access Token saved! You may now close this tab."


def open_browser():
    url = "http://localhost:5000/"
    logging.info(f"Opening web browser to {url}")
    webbrowser.open_new(url)


if __name__ == '__main__':
    logging.info("Current working directory: %s", os.getcwd())
    threading.Timer(1, open_browser).start()
    logging.info("Starting Flask auth_app server on port 5000")
    app.run(debug=True, port=5000)
