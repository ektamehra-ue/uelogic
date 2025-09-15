import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from api.models import Organization, Meter, VirtualAllocation


def norm(s):
    return (s or "").strip()
