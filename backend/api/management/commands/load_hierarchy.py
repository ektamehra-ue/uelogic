import csv
from collections import defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from api.models import Organization, Building, Account, Meter


def norm(s):
    return (s or "").strip()

class Command(BaseCommand):
    help = "Load Organizations, Buildings, Accounts, and Meters from a hierarchy CSV."

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str, help="Path to hierarchy CSV (e.g., ../../data/hierarchy.csv)")
        parser.add_argument("--dry-run", action="store_true", help="Parse and report only, no DB writes")
        parser.add_argument("--delimiter", default=",", help="CSV delimiter (default ,)")

    def handle(self, *args, **options):
        csv_path = Path(options["csv_path"])
        if not csv_path.exists():
            raise CommandError(f"CSV file not found: {csv_path}")

        dry_run = options["dry_run"]
        delimiter = options["delimiter"]

        # Column aliases to be flexible with headings
        aliases = {
            "organization": ["organization", "org", "organization name", "org name"],
            "building": ["building", "building name", "site", "site name"],
            "account": ["account", "account name", "customer", "customer name"],
            "identifier": {"identifier", "meter_identifier", "meter id", "meter", "meter_ref"},
            "external_id": {"external_id", "external id", "source_id"},
            "meter_type": {"meter_type", "type"},
            "parent_identifier": {"parent_identifier", "parent id", "parent"},
            "unit": {"unit", "uom"},
            "is_active": {"is_active", "active", "enabled"},
        }

        def resolve_key(header_to_idx, key):
            for alias in aliases[key]:
                for h in header_to_idx:
                    if h.lower().strip() == alias:
                        return h
            return None


