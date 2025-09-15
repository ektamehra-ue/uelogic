import csv
from collections import defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from api.models import Organization, Building, Account, Meter


def norm(s):
    return (s or "").strip()
