# reporting/google_drive_upload_zapier_send.py

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

from monthly_punto_valle_sales import generate_monthly_report

# Load environment variables from secrets.env if it exists
def load_secrets_env():
    """Load environment variables from utils/secrets.env if it exists."""
    secrets_file = Path(__file__).parent.parent / "utils" / "secrets.env"
    if secrets_file.exists():
        with open(secrets_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue
                # Parse KEY=VALUE format
                if "=" in line and not line.startswith("export"):
                    key, value = line.split("=", 1)
                    # Remove quotes if present
                    value = value.strip('"\'')
                    os.environ.setdefault(key.strip(), value)

# Load secrets on import
load_secrets_env()

# Solo acceso a archivos que tu app cree/suba
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

MONTHS_ES = [
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre",
]

# File paths for Google credentials (these files should be in .gitignore)
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
TOKEN_FILE = os.getenv("GOOGLE_TOKEN_FILE", "token.json")

# Google Drive folder ID for uploads (read from environment variable)
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
if not DRIVE_FOLDER_ID:
    raise ValueError(
        "GOOGLE_DRIVE_FOLDER_ID environment variable is not set. "
        "Please set it in your environment or utils/secrets.env file."
    )


def get_drive_service():
    """Get authenticated Google Drive service."""
    # Try to find credentials file in multiple locations
    credentials_file = Path(CREDENTIALS_FILE)
    if not credentials_file.exists():
        # Try in project root
        project_root = Path(__file__).parent.parent
        credentials_file = project_root / CREDENTIALS_FILE
        if not credentials_file.exists():
            # Try expanded user path
            credentials_file = Path(os.path.expanduser(CREDENTIALS_FILE))
            if not credentials_file.exists():
                raise FileNotFoundError(
                    f"No encuentro {CREDENTIALS_FILE}. "
                    f"Ponlo en el directorio raíz del proyecto o en el directorio actual."
                )
    
    # Try to find token file in multiple locations
    token_file = Path(TOKEN_FILE)
    if not token_file.exists():
        # Try in project root
        project_root = Path(__file__).parent.parent
        token_file = project_root / TOKEN_FILE
        if not token_file.exists():
            # Try expanded user path
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


def previous_month_text(today: date | None = None) -> tuple[str, int, int]:
    """Get previous month name, year, and month number."""
    today = today or date.today()
    y, m = today.year, today.month
    if m == 1:
        y -= 1
        m = 12
    else:
        m -= 1
    return MONTHS_ES[m - 1], y, m  # (mes, año, numero_mes)


def cleanup_old_temp_files(temp_dir: Path, current_month_date: date):
    """
    Remove old Excel files from temp directory that are not from the current month.
    This prevents confusion when the script runs automatically next month.
    """
    if not temp_dir.exists():
        return
    
    current_month_str = current_month_date.strftime("%Y-%m")
    
    # Find all Excel files in temp directory
    for file_path in temp_dir.glob("*.xlsx"):
        # Check if file is from a different month by examining filename
        # Files are named: "Panem Punto Valle - Ventas {Month} {Year}.xlsx"
        file_name = file_path.name
        
        # Extract year-month from filename if possible, or use file modification time
        try:
            # Try to extract date from filename pattern
            # If filename contains month/year, we can check it
            # Otherwise, use file modification time
            file_mtime = date.fromtimestamp(file_path.stat().st_mtime)
            file_month_str = file_mtime.strftime("%Y-%m")
            
            # If file is from a different month, delete it
            if file_month_str != current_month_str:
                print(f"Removing old file: {file_path.name} (from {file_month_str})")
                file_path.unlink()
        except Exception as e:
            # If we can't determine the date, be conservative and keep the file
            print(f"Warning: Could not determine age of {file_path.name}: {e}")


def main():
    """Main function: generate report and upload to Google Drive."""
    # Calculate the target month (last month)
    from monthly_punto_valle_sales import get_last_month_range
    
    start_date, _ = get_last_month_range()
    temp_dir = Path("data") / "a_raw" / "order_times" / "temp"
    
    # Clean up old files from previous months before generating new report
    print("Cleaning up old files from temp directory...")
    cleanup_old_temp_files(temp_dir, start_date)
    
    # Generate the monthly report for Punto Valle
    # This downloads order_times and saves to data/a_raw/order_times/temp
    print("Generating monthly report for Punto Valle...")
    try:
        output_path = generate_monthly_report(
            sucursal="Punto Valle",
            data_root="data",
            branches_file="./utils/sucursales.json",
        )
        print(f"Report generated: {output_path}")
    except Exception as e:
        print(f"Error generating report: {e}", file=sys.stderr)
        return 1

    # Verify the file exists
    if not output_path.exists():
        print(f"Error: Generated file does not exist: {output_path}", file=sys.stderr)
        return 1

    # Get Google Drive service
    print("Authenticating with Google Drive...")
    try:
        service = get_drive_service()
    except Exception as e:
        print(f"Error authenticating with Google Drive: {e}", file=sys.stderr)
        return 1

    # Upload to Google Drive
    print(f"Uploading {output_path.name} to Google Drive...")
    try:
        file_id, uploaded_name, view_link = upload_file(
            service, str(output_path), DRIVE_FOLDER_ID
        )

        print("OK ✅ Subido a Drive")
        print("Nombre:", uploaded_name)
        print("File ID:", file_id)
        print("Link:", view_link or "(no disponible)")
        return 0
    except Exception as e:
        print(f"Error uploading to Google Drive: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
