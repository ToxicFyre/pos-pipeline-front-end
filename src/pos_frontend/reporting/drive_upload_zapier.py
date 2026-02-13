# pos_frontend.reporting.drive_upload_zapier

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from pos_frontend.config.paths import get_project_root, load_secrets_env
from pos_frontend.reporting.monthly_pv_sales import generate_monthly_report, get_last_month_range

# Load secrets on import (project root + utils/secrets.env)
load_secrets_env()

# Solo acceso a archivos que tu app cree/suba
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# File paths for Google credentials (these files should be in .gitignore)
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE = os.getenv("GOOGLE_TOKEN_FILE", "token.json")

# Google Drive folder ID for uploads (read from environment variable)
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
if not DRIVE_FOLDER_ID:
    raise ValueError(
        "GOOGLE_DRIVE_FOLDER_ID environment variable is not set. "
        "Please set it in your environment or secrets.env file."
    )


def get_drive_service():
    """Get authenticated Google Drive service."""
    project_root = get_project_root()

    credentials_file = Path(CREDENTIALS_FILE)
    if not credentials_file.is_absolute():
        credentials_file = project_root / CREDENTIALS_FILE
    if not credentials_file.exists():
        credentials_file = Path(os.path.expanduser(CREDENTIALS_FILE))
    if not credentials_file.exists():
        raise FileNotFoundError(
            f"No encuentro {CREDENTIALS_FILE}. "
            f"Ponlo en el directorio raíz del proyecto o en el directorio actual."
        )

    token_file = Path(TOKEN_FILE)
    if not token_file.is_absolute():
        token_file = project_root / TOKEN_FILE
    if not token_file.exists():
        token_file = Path(os.path.expanduser(TOKEN_FILE))

    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(str(token_file), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file), SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_file, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


def upload_file(service, local_path: str, drive_folder_id: str | None = None):
    """Upload a file to Google Drive."""
    path = Path(local_path)
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {local_path}")

    meta = {"name": path.name}
    if drive_folder_id:
        meta["parents"] = [drive_folder_id]

    media = MediaFileUpload(str(path), resumable=True)
    created = (
        service.files()
        .create(body=meta, media_body=media, fields="id,name,webViewLink")
        .execute()
    )
    return created["id"], created["name"], created.get("webViewLink")


def cleanup_old_temp_files(temp_dir: Path, current_month_date: date):
    """Remove old Excel files from temp directory that are not from the current month."""
    if not temp_dir.exists():
        return

    current_month_str = current_month_date.strftime("%Y-%m")

    for file_path in temp_dir.glob("*.xlsx"):
        try:
            file_mtime = date.fromtimestamp(file_path.stat().st_mtime)
            file_month_str = file_mtime.strftime("%Y-%m")

            if file_month_str != current_month_str:
                print(f"Removing old file: {file_path.name} (from {file_month_str})")
                file_path.unlink()
        except Exception as e:
            print(f"Warning: Could not determine age of {file_path.name}: {e}")


def main():
    """Main function: generate report and upload to Google Drive."""
    project_root = get_project_root()
    start_date, _ = get_last_month_range()
    temp_dir = project_root / "data" / "a_raw" / "order_times" / "temp"

    print("Cleaning up old files from temp directory...")
    cleanup_old_temp_files(temp_dir, start_date)

    print("Generating monthly report for Punto Valle...")
    try:
        output_path = generate_monthly_report(
            sucursal="Punto Valle",
            data_root=str(project_root / "data"),
            branches_file=str(project_root / "sucursales.json"),
        )
        print(f"Report generated: {output_path}")
    except Exception as e:
        print(f"Error generating report: {e}", file=sys.stderr)
        return 1

    if not output_path.exists():
        print(f"Error: Generated file does not exist: {output_path}", file=sys.stderr)
        return 1

    print("Authenticating with Google Drive...")
    try:
        service = get_drive_service()
    except Exception as e:
        print(f"Error authenticating with Google Drive: {e}", file=sys.stderr)
        return 1

    print(f"Uploading {output_path.name} to Google Drive...")
    try:
        file_id, uploaded_name, view_link = upload_file(
            service, str(output_path), DRIVE_FOLDER_ID
        )

        print("OK - Subido a Drive")
        print("Nombre:", uploaded_name)
        print("File ID:", file_id)
        print("Link:", view_link or "(no disponible)")
        return 0
    except Exception as e:
        print(f"Error uploading to Google Drive: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
