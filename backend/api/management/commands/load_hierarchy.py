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
        
        
        # Read CSV
        with csv_path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f, delimiter=delimiter)
            rows = list(reader)

        if not rows:
            self.stdout.write(self.style.WARNING("Empty CSV"))
            return

        headers = [h.strip() for h in rows[0]]
        header_to_idx = {h: i for i, h in enumerate(headers)}

        # Map column names found
        col = {k: resolve_key(header_to_idx, k) for k in aliases}
        required = ["org", "building", "identifier", "meter_type", "unit"]
        missing = [k for k in required if not col[k]]
        if missing:
            raise CommandError(f"Missing required column(s): {', '.join(missing)}. Found headers: {headers}")



