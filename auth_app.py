import os
import webbrowser
import logging
from flask import Flask, redirect, request
import requests
from dotenv import load_dotenv
import threading

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ---------- Load environment variables ----------
load_dotenv()

# Upstox creds (from .env)
API_KEY = os.getenv("UPSTOX_API_KEY")       # Upstox client_id
API_SECRET = os.getenv("UPSTOX_API_SECRET") # Upstox client_secret
REDIRECT_URI = os.getenv("UPSTOX_REDIRECT_URI")

if not API_KEY or not API_SECRET or not REDIRECT_URI:
    log.warning(
        "UPSTOX_API_KEY / UPSTOX_API_SECRET / UPSTOX_REDIRECT_URI "
        "not fully set in .env – OAuth will fail."
    )

app = Flask(__name__)


@app.route('/')
def login():
    """Redirect user to Upstox OAuth login page."""
    auth_url = (
        "https://api.upstox.com/v2/login/authorization/dialog"
        f"?client_id={API_KEY}"
        f"&redirect_uri={REDIRECT_URI}"
        "&response_type=code"
    )
    log.info("Redirecting user to Upstox OAuth login page")
    return redirect(auth_url)


@app.route('/callback')
def callback():
    """Handle Upstox OAuth redirect and exchange code for access token."""
    code = request.args.get('code')
    if not code:
        log.error("No authorization code received in callback")
        return "❌ No authorization code received. Please retry."

    log.info("Authorization code received: %s", code)

    token_url = "https://api.upstox.com/v2/login/authorization/token"

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

    # --- Exchange code for token ---
    try:
        resp = requests.post(token_url, data=payload, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error("Request failed during token exchange: %r", e)
        return f"❌ Request failed: {e}"

    token_data = resp.json()
    log.info("Token response received: %s", token_data)

    access_token = token_data.get("access_token")
    if not access_token:
        log.error("Invalid token response: %s", token_data)
        return f"❌ Invalid token response: {token_data}"

    # --- Save token files ---
    token_path = os.path.abspath("token.txt")
    refresh_path = os.path.abspath("refresh_token.txt")

    try:
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(access_token)

        # Upstox currently doesn’t give a usable refresh token; keep file empty
        with open(refresh_path, "w", encoding="utf-8") as f:
            f.write("")
    except OSError as e:
        log.error("Failed to save token files on disk", exc_info=True)
        return f"❌ Failed to save token files on disk: {repr(e)}"

    log.info("Access token saved successfully at %s", token_path)
    return "✅ Access Token saved! You may now close this tab."


def open_browser():
    url = "http://localhost:5000/"
    log.info("Opening web browser to %s", url)
    webbrowser.open_new(url)


if __name__ == '__main__':
    log.info("Current working directory: %s", os.getcwd())

    # --------------------------------------
    # FIX: Only open browser in reloader child
    # --------------------------------------
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Timer(1, open_browser).start()

    log.info("Starting Flask auth_app server on port 5000")
    app.run(debug=True, port=5000)
