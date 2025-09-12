# backend/core/settings.py
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("POSTGRES_DB", "uelogic"),
        "USER": os.getenv("POSTGRES_USER", "postgres"),
        "PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
        # IMPORTANT:
        # - inside Docker, Django should use host "db" and port 5432
        # - if you run locally (no Docker), use host "localhost" and port 5433 (see Step 5)
        "HOST": os.getenv("POSTGRES_HOST", "db"),
        "PORT": os.getenv("POSTGRES_PORT", "5432"),
    }
}

TIME_ZONE = "Europe/London"
USE_TZ = True
