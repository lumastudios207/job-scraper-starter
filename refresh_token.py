"""Refresh the Google Drive OAuth token.

Run this whenever runner.py fails with a RefreshError:
    python refresh_token.py

It opens the browser-based OAuth flow and writes a fresh token.json.
"""
from __future__ import annotations

from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

BASE_DIR = Path(__file__).resolve().parent
TOKEN_PATH = BASE_DIR / "token.json"
CREDENTIALS_PATH = BASE_DIR / "credentials.json"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def main() -> None:
    if not CREDENTIALS_PATH.exists():
        raise SystemExit(f"Missing {CREDENTIALS_PATH}. Place your OAuth client credentials there first.")

    if TOKEN_PATH.exists():
        TOKEN_PATH.unlink()

    flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")
    TOKEN_PATH.write_text(creds.to_json())
    print("Token refreshed successfully — you can now run python runner.py")


if __name__ == "__main__":
    main()
