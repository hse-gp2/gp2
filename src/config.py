import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
env_file = BASE_DIR / ".env"
if env_file.exists():
    load_dotenv(env_file)

DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
SRC_DIR = BASE_DIR / "src"

DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

GOOGLE_BOOKS_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY", "")

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
    },
    "handlers": {
        "file": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "standard",
            "filename": LOGS_DIR / "project.log",
            "mode": "a",
        },
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
        },
    },
    "loggers": {
        "": {
            "handlers": ["file", "console"],
            "level": "INFO",
            "propagate": False
        }
    }
}

SCRAPING_DELAY = 0.1
SCRAPING_TIMEOUT = 30

API_MAX_RESULTS = 40
API_DELAY = 1.0
