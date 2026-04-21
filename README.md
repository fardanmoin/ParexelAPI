# Humanity Shift Importer

A web tool that converts schedule templates (CSV / XLSX) and pushes shifts directly into a Humanity account via the v2 API.

## How it works

1. User enters their own **Humanity App ID + App Secret** (OAuth 2.0 credentials created in Humanity Settings → API v2)
2. User enters a **location name** (must match a location that already exists in their Humanity account)
3. User uploads a CSV or XLSX schedule file
4. Backend parses and previews the rows
5. User clicks **Send to Humanity** — each shift is created via `POST /api/v2/shifts`

## Field mapping

| Source column         | → | Humanity shift field              |
|-----------------------|---|-----------------------------------|
| Date                  | → | `start_date` + `end_date`         |
| Task Name             | → | Position (`schedule` ID) + `title`|
| Planned Start         | → | `start_time`                      |
| Planned End           | → | `end_time`                        |
| Required Skills       | → | `skill_requirement`               |
| Study ID              | → | `notes`                           |
| Number Of Resources   | → | `employees` (open slots)          |
| Group                 | → | *(ignored)*                       |
| User-typed location   | → | `location`                        |

Overnight shifts (end time ≤ start time) auto-bump the end date by one day.

## Important: never auto-create

The tool **never auto-creates** positions, skills, or locations in Humanity. If any of those references don't already exist in the client's account, that row is skipped and reported.

## Local development

```bash
python -m venv venv
source venv/bin/activate          # or: venv\Scripts\activate on Windows
pip install -r requirements.txt
uvicorn main:app --reload
```

Then open http://localhost:8000

## Deploy to Render (free)

1. Push this folder to a new GitHub repo
2. Go to [render.com](https://render.com) → New → Blueprint
3. Connect the GitHub repo
4. Render auto-detects `render.yaml` and deploys
5. Your tool is live at `https://<service-name>.onrender.com`

No environment variables needed — each client supplies their own Humanity credentials at runtime.

## Privacy

- Humanity credentials are sent with each request for authentication only
- Nothing is stored on the server (no database, no session, no logs of credentials)
- File contents are processed in memory and discarded after the response
