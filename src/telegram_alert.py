# %%
import requests
from pathlib import Path

BOT_TOKEN = 
CHAT_ID = 
MESSAGE = "Telegram test: your bot is working!"

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

def send_telegram_message(text: str):
    """Send a text message to your Telegram chat."""
    try:
        url = f"{BASE_URL}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": text}
        r = requests.post(url, data=payload)
        r.raise_for_status()
    except Exception as e:
        print(f"[Telegram Error] Failed to send message: {e}")


def send_telegram_file(file_path: str, caption: str = ""):
    """
    Send any file (image, csv, txt, log, pdf, etc.) to Telegram.
    Automatically handled as a 'document'.
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            print(f"[Telegram Error] File not found: {file_path}")
            return

        url = f"{BASE_URL}/sendDocument"
        with open(file_path, "rb") as f:
            files = {"document": f}
            data = {"chat_id": CHAT_ID, "caption": caption}
            r = requests.post(url, files=files, data=data)
            r.raise_for_status()
        print(f"[Telegram] File sent: {file_path.name}")
    except Exception as e:
        print(f"[Telegram Error] Failed to send file: {e}")
