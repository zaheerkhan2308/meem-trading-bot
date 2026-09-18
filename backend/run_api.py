"""Run the API without the scheduler (useful for dashboard development)."""
import uvicorn
from .api import app
from .logging_config import configure_logging
from .config import API_HOST, PORT

if __name__ == "__main__":
    configure_logging()
    uvicorn.run(app, host=API_HOST, port=PORT)
