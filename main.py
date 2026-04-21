"""
Humanity Shift Importer - Backend API
-------------------------------------
FastAPI service that:
  1. Accepts a CSV/XLSX schedule file + client's Humanity OAuth credentials
  2. Parses and validates the rows
  3. On confirmation, creates shifts in Humanity via v2 API
  4. Never stores credentials or data on the server (in-memory only, per request)
"""

from __future__ import annotations

import io
import re
from datetime import datetime, timedelta
from typing import Any

import httpx
import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Humanity API constants
# ---------------------------------------------------------------------------
HUMANITY_TOKEN_URL = "https://www.humanity.com/oauth2/token.php"
HUMANITY_API_BASE = "https://www.humanity.com/api/v2"
REQUEST_TIMEOUT = 30.0

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="Humanity Shift Importer")

# Permissive CORS — this is a self-contained tool; frontend and backend are
# served from the same origin in production, but this keeps dev simple.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _norm(s: Any) -> str:
    """Normalise a string for loose equality (trim + lowercase + collapse whitespace)."""
    return re.sub(r"\s+", " ", str(s or "").strip()).lower()


def _norm_header(s: Any) -> str:
    """Stricter normalisation for matching header names."""
    return re.sub(r"[\s_\-]+", "", str(s or "").strip().lower())


def _get_field(row: dict, variants: list[str]) -> str:
    """Return the value for the first column whose header matches any variant."""
    for key, value in row.items():
        if _norm_header(key) in {_norm_header(v) for v in variants}:
            if value is None:
                return ""
            return str(value).strip()
    return ""


def _parse_time(raw: Any) -> tuple[int, int] | None:
    """Parse a time value into (hour_24, minute). Returns None if unparseable."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None

    # Excel may hand back a float representing fraction-of-a-day
    try:
        as_num = float(s)
        if 0 <= as_num < 1:
            total_min = round(as_num * 24 * 60)
            return (total_min // 60) % 24, total_min % 60
    except ValueError:
        pass

    s = s.lower().replace("  ", " ").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})(?::\d{2})?\s*(am|pm)?$", s)
    if not m:
        return None

    hour = int(m.group(1))
    minute = int(m.group(2))
    ampm = m.group(3)

    if hour > 23 or minute > 59:
        return None

    if ampm == "am" and hour == 12:
        hour = 0
    elif ampm == "pm" and hour != 12:
        hour += 12

    return hour, minute


def _fmt_time_humanity(h: int, m: int) -> str:
    """Format to Humanity's expected time format, '4:45am' style (lowercase, no space)."""
    suffix = "pm" if h >= 12 else "am"
    h12 = h % 12 or 12
    return f"{h12}:{m:02d}{suffix}"


def _parse_date(raw: Any) -> datetime | None:
    """Parse a date value (various formats) into a datetime at midnight."""
    if raw is None:
        return None

    # Already a datetime/pandas Timestamp
    if isinstance(raw, (datetime, pd.Timestamp)):
        return datetime(raw.year, raw.month, raw.day)

    s = str(raw).strip()
    if not s:
        return None

    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    # Last-ditch attempt via pandas
    try:
        dt = pd.to_datetime(s)
        return datetime(dt.year, dt.month, dt.day)
    except Exception:
        return None


def _fmt_date_humanity(d: datetime) -> str:
    """Format a datetime to Humanity's expected date, MM/DD/YYYY."""
    return f"{d.month}/{d.day}/{d.year}"


# ---------------------------------------------------------------------------
# Core row-parsing logic (shared by preview and send)
# ---------------------------------------------------------------------------
def _read_file_to_rows(file_bytes: bytes, filename: str) -> list[dict]:
    """Read an uploaded CSV or XLSX file into a list of row dicts."""
    lower = filename.lower()
    try:
        if lower.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_bytes), dtype=str, keep_default_na=False)
        elif lower.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(file_bytes), dtype=str, keep_default_na=False)
        else:
            raise HTTPException(
                status_code=400,
                detail="Unsupported file type. Please upload .csv, .xlsx, or .xls",
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}") from e

    return df.to_dict(orient="records")


def _parse_rows(rows: list[dict], location: str) -> dict:
    """
    Parse source rows into Humanity-ready shift payloads.

    Returns:
        {
            "parsed": [ {row_num, payload, display, task_name, required_skills, ...}, ... ],
            "errors": [ {row_num, message}, ... ],
            "overnight_count": int,
        }
    """
    parsed: list[dict] = []
    errors: list[dict] = []
    overnight_count = 0

    for i, row in enumerate(rows):
        row_num = i + 2  # +1 for header, +1 for 1-indexed display

        # Skip fully empty rows
        if all(
            (v is None or str(v).strip() == "") for v in row.values()
        ):
            continue

        date_raw = _get_field(row, ["Date"])
        task_name = _get_field(row, ["Task Name", "TaskName"])
        planned_start = _get_field(row, ["Planned Start", "PlannedStart"])
        planned_end = _get_field(row, ["Planned End", "PlannedEnd"])
        required_skills = _get_field(
            row, ["Required Skills", "RequiredSkills"]
        )
        num_resources = _get_field(
            row, ["Number Of Resources", "NumberOfResources", "Number of Resources"]
        )
        study_id = _get_field(row, ["Study ID", "StudyID", "Study Id"])

        # Validate required fields
        parsed_date = _parse_date(date_raw)
        parsed_start = _parse_time(planned_start)
        parsed_end = _parse_time(planned_end)

        if not parsed_date:
            errors.append({"row_num": row_num, "message": f"Unreadable Date: '{date_raw}'"})
            continue
        if not parsed_start:
            errors.append({"row_num": row_num, "message": f"Unreadable Planned Start: '{planned_start}'"})
            continue
        if not parsed_end:
            errors.append({"row_num": row_num, "message": f"Unreadable Planned End: '{planned_end}'"})
            continue
        if not task_name:
            errors.append({"row_num": row_num, "message": "Missing Task Name (required for Position)"})
            continue
        if not required_skills:
            errors.append({"row_num": row_num, "message": "Missing Required Skills"})
            continue

        # Overnight detection
        start_total = parsed_start[0] * 60 + parsed_start[1]
        end_total = parsed_end[0] * 60 + parsed_end[1]
        end_date = parsed_date
        is_overnight = end_total <= start_total
        if is_overnight:
            end_date = parsed_date + timedelta(days=1)
            overnight_count += 1

        # Employees needed
        employees_needed = 1
        if num_resources:
            try:
                employees_needed = max(1, int(float(num_resources)))
            except ValueError:
                pass

        parsed.append(
            {
                "row_num": row_num,
                "task_name": task_name,            # becomes Position + title
                "required_skills": required_skills,  # becomes skill_requirement
                "location": location,
                "study_id": study_id,
                "display": {
                    "date": _fmt_date_humanity(parsed_date),
                    "end_date": _fmt_date_humanity(end_date),
                    "start_time": _fmt_time_humanity(*parsed_start),
                    "end_time": _fmt_time_humanity(*parsed_end),
                    "task_name": task_name,
                    "required_skills": required_skills,
                    "study_id": study_id,
                    "employees_needed": employees_needed,
                    "location": location,
                    "overnight": is_overnight,
                },
                # Partial payload; IDs (schedule, location, skill) filled in at send time
                "raw": {
                    "start_date": _fmt_date_humanity(parsed_date),
                    "end_date": _fmt_date_humanity(end_date),
                    "start_time": _fmt_time_humanity(*parsed_start),
                    "end_time": _fmt_time_humanity(*parsed_end),
                    "title": task_name,
                    "notes": study_id,
                    "employees": employees_needed,
                },
            }
        )

    return {"parsed": parsed, "errors": errors, "overnight_count": overnight_count}


# ---------------------------------------------------------------------------
# Humanity API client (in-memory, per-request)
# ---------------------------------------------------------------------------
class HumanityClient:
    """Thin wrapper around Humanity v2 REST API for the operations we need."""

    def __init__(self, access_token: str):
        self.access_token = access_token
        self.session = httpx.Client(
            base_url=HUMANITY_API_BASE,
            timeout=REQUEST_TIMEOUT,
            headers={"Authorization": f"Bearer {access_token}"},
        )

    def close(self) -> None:
        self.session.close()

    def _get_all(self, path: str) -> list[dict]:
        """GET a list endpoint, handling simple pagination if present."""
        items: list[dict] = []
        page = 1
        while True:
            r = self.session.get(path, params={"page": page, "limit": 200})
            r.raise_for_status()
            data = r.json()

            # Humanity sometimes wraps the list, sometimes not — handle both
            if isinstance(data, dict) and "data" in data:
                chunk = data["data"]
            elif isinstance(data, list):
                chunk = data
            else:
                chunk = []

            if not chunk:
                break
            items.extend(chunk)
            if len(chunk) < 200:
                break
            page += 1
        return items

    def get_positions(self) -> list[dict]:
        return self._get_all("/positions")

    def get_locations(self) -> list[dict]:
        return self._get_all("/locations")

    def get_skills(self) -> list[dict]:
        try:
            return self._get_all("/skills")
        except httpx.HTTPStatusError:
            # Some accounts may not expose a /skills list endpoint
            return []

    def create_shift(self, payload: dict) -> dict:
        r = self.session.post("/shifts", json=payload)
        if r.status_code >= 400:
            try:
                err = r.json()
            except Exception:
                err = r.text
            raise RuntimeError(f"HTTP {r.status_code}: {err}")
        return r.json()


def _get_access_token(app_id: str, app_secret: str) -> str:
    """Exchange App ID + App Secret for an access token using OAuth 2.0 client_credentials."""
    try:
        r = httpx.post(
            HUMANITY_TOKEN_URL,
            data={
                "client_id": app_id,
                "client_secret": app_secret,
                "grant_type": "client_credentials",
            },
            timeout=REQUEST_TIMEOUT,
        )
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Could not reach Humanity for authentication: {e}",
        ) from e

    if r.status_code != 200:
        raise HTTPException(
            status_code=401,
            detail=f"Humanity authentication failed ({r.status_code}). Check your App ID and App Secret.",
        )

    token = r.json().get("access_token")
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Humanity returned no access_token. Check your App ID and App Secret.",
        )
    return token


def _find_by_name(items: list[dict], name: str) -> dict | None:
    """Find an item in a list by case-insensitive 'name' match."""
    target = _norm(name)
    for it in items:
        n = it.get("name") or it.get("title") or ""
        if _norm(n) == target:
            return it
    return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.post("/api/preview")
async def preview(file: UploadFile = File(...), location: str = Form(...)):
    """Parse the uploaded file and return the rows that would be sent."""
    if not location.strip():
        raise HTTPException(status_code=400, detail="Location is required.")

    file_bytes = await file.read()
    rows = _read_file_to_rows(file_bytes, file.filename or "")

    if not rows:
        raise HTTPException(status_code=400, detail="The file appears to be empty.")

    # Sanity-check the headers
    headers = {_norm_header(k) for k in rows[0].keys()}
    expected = {"date", "taskname", "plannedstart", "plannedend"}
    if len(headers & expected) < 2:
        raise HTTPException(
            status_code=400,
            detail="This file does not look like the expected schedule template. "
            "Missing columns like Date, Task Name, Planned Start, Planned End.",
        )

    result = _parse_rows(rows, location.strip())

    return {
        "rows": [
            {
                "row_num": p["row_num"],
                **p["display"],
            }
            for p in result["parsed"]
        ],
        "errors": result["errors"],
        "overnight_count": result["overnight_count"],
        "total_valid": len(result["parsed"]),
    }


class SendResult(BaseModel):
    created: int
    failed: int
    overnight_count: int
    results: list[dict]
    errors: list[dict]


@app.post("/api/send")
async def send(
    file: UploadFile = File(...),
    location: str = Form(...),
    app_id: str = Form(...),
    app_secret: str = Form(...),
):
    """
    Parse the file and create each shift in Humanity.
    Never auto-creates positions or skills — rows with missing refs fail.
    """
    if not app_id.strip() or not app_secret.strip():
        raise HTTPException(status_code=400, detail="App ID and App Secret are required.")
    if not location.strip():
        raise HTTPException(status_code=400, detail="Location is required.")

    file_bytes = await file.read()
    rows = _read_file_to_rows(file_bytes, file.filename or "")
    if not rows:
        raise HTTPException(status_code=400, detail="The file appears to be empty.")

    parsed_result = _parse_rows(rows, location.strip())
    parsed = parsed_result["parsed"]
    errors = list(parsed_result["errors"])

    if not parsed:
        return SendResult(
            created=0,
            failed=0,
            overnight_count=parsed_result["overnight_count"],
            results=[],
            errors=errors,
        )

    # Auth + fetch reference data once
    token = _get_access_token(app_id.strip(), app_secret.strip())
    client = HumanityClient(token)

    try:
        positions = client.get_positions()
        locations = client.get_locations()
        skills = client.get_skills()
    except httpx.HTTPStatusError as e:
        client.close()
        raise HTTPException(
            status_code=502,
            detail=f"Could not fetch reference data from Humanity: {e.response.status_code}",
        ) from e

    location_match = _find_by_name(locations, location.strip())
    if not location_match:
        client.close()
        available = ", ".join(sorted({l.get("name", "") for l in locations if l.get("name")}))
        raise HTTPException(
            status_code=400,
            detail=f"Location '{location.strip()}' not found in your Humanity account. "
            f"Available: {available or '(none)'}",
        )
    location_id = location_match.get("id")

    results = []
    created = 0
    failed = 0

    for p in parsed:
        row_num = p["row_num"]
        task_name = p["task_name"]
        required_skills = p["required_skills"]

        # Resolve Position (Task Name) — never auto-create
        position = _find_by_name(positions, task_name)
        if not position:
            failed += 1
            errors.append({
                "row_num": row_num,
                "message": f"Position '{task_name}' does not exist in Humanity. "
                           "Create it in Humanity first (never auto-create mode).",
            })
            results.append({"row_num": row_num, "status": "failed", "reason": "missing position"})
            continue

        # Resolve Skill (Required Skills) — never auto-create
        # Support multiple values separated by ; or ,
        skill_values = [s.strip() for s in re.split(r"[;,]", required_skills) if s.strip()]
        skill_ids = []
        missing_skills = []
        for sv in skill_values:
            skill = _find_by_name(skills, sv)
            if skill:
                skill_ids.append(skill.get("id"))
            else:
                missing_skills.append(sv)

        if missing_skills:
            failed += 1
            errors.append({
                "row_num": row_num,
                "message": f"Skill(s) not found in Humanity: {', '.join(missing_skills)}",
            })
            results.append({"row_num": row_num, "status": "failed", "reason": f"missing skill(s): {', '.join(missing_skills)}"})
            continue

        payload = {
            "schedule": position.get("id"),
            "location": location_id,
            "start_date": p["raw"]["start_date"],
            "end_date": p["raw"]["end_date"],
            "start_time": p["raw"]["start_time"],
            "end_time": p["raw"]["end_time"],
            "title": p["raw"]["title"],
            "notes": p["raw"]["notes"],
            "employees": p["raw"]["employees"],
        }
        if skill_ids:
            payload["skill_requirement"] = skill_ids

        try:
            client.create_shift(payload)
            created += 1
            results.append({"row_num": row_num, "status": "created"})
        except Exception as e:
            failed += 1
            errors.append({"row_num": row_num, "message": f"Humanity API error: {e}"})
            results.append({"row_num": row_num, "status": "failed", "reason": str(e)})

    client.close()

    return SendResult(
        created=created,
        failed=failed,
        overnight_count=parsed_result["overnight_count"],
        results=results,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Serve the frontend
# ---------------------------------------------------------------------------
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.get("/healthz")
async def healthz():
    return {"ok": True}
