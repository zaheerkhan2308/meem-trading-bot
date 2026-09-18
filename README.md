# Meem

Meem is split into an API/trading runtime and a standalone React dashboard.

## Local development

The backend loads settings only from the file selected by `MEEM_ENV_FILE`.
Local development uses `.env.local`; production uses `.env.production`.
Both files are ignored by git and must be populated with real credentials.

From the repository root, start the backend (which retains the existing scheduler and trading callbacks):

```powershell
python -m backend.main
```

Then, in another terminal, start the frontend:

```powershell
cd frontend
npm install
npm run dev
```

The Vite dashboard is available at `http://localhost:5173`. Its `/api` and
`/ws` requests proxy to the backend on port 8000. For an API process without
the scheduler, run `python -m backend.run_api` from the repository root.

### Full local launch

1. Add a real Neon connection URL to `NEON_DATABASE_URL` in `.env.local`.
2. Create and populate the Python environment:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\python.exe -m pip install -r backend\requirements.txt
   ```

3. Start the full backend (API plus existing scheduler) from the repository root:

   ```powershell
   .\.venv\Scripts\python.exe -m backend.main
   ```

   The JSON API and WebSocket are on `http://localhost:8000`, with health at
   `http://localhost:8000/health` and API documentation at `http://localhost:8000/docs`.

4. In a second terminal, start the dashboard:

   ```powershell
   cd frontend
   npm install
   npm run dev
   ```

   Open `http://localhost:5173`. Vite proxies `/api` and `/ws` to port 8000.

## API

- `GET /api/portfolio`, `/api/trades`, `/api/scan-log`, `/api/watchlist`, `/api/circuit-breaker`, `/api/chart`
- `POST /api/kill-switch`, `POST /api/dry-run`
- `WS /ws`

Set `API_CORS_ORIGINS` in `.env` to a comma-separated list of approved
frontend origins for direct cross-origin production access.
