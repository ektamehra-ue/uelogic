import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from api.models import Organization, Meter, VirtualAllocation


def norm(s):
    return (s or "").strip()

class Command(BaseCommand):
    help = "Load VirtualAllocation rows from a formulas CSV (parent_identifier, child_identifier, percent)."

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str, help="Path to formulas CSV (e.g., ../../data/formulas.csv)")
        parser.add_argument("--org", type=str, default=None, help="Organization name (if CSV doesn’t include org column)")
        parser.add_argument("--dry-run", action="store_true", help="Parse and report only, no DB writes")
        parser.add_argument("--delimiter", default=",", help="CSV delimiter (default ,)")

    def handle(self, *args, **opts):
        csv_path = Path(opts["csv_path"]).resolve()
        if not csv_path.exists():
            raise CommandError(f"CSV not found: {csv_path}")

        org_name_cli = opts["org"]
        dry = opts["dry_run"]
        delimiter = opts["delimiter"]

        # Read header using csv.reader (CSV-safe)
        with csv_path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f, delimiter=delimiter)
            try:
                headers = next(reader)
            except StopIteration:
                self.stdout.write(self.style.WARNING("Empty CSV"))
                return
        headers_lower = [h.strip().lower() for h in headers]

        # Column index helper
        def idx_of(*candidates):
            for c in candidates:
                c_low = c.strip().lower()
                if c_low in headers_lower:
                    return headers_lower.index(c_low)
            return None

        # Detect columns (org optional)
        org_idx = idx_of("org", "organization", "organisation")
        parent_idx = idx_of("parent_identifier", "parent id", "parent", "parent_meter")
        child_idx = idx_of("child_identifier", "child id", "child", "child_meter")
        percent_idx = idx_of("percent", "allocation", "pct", "percentage")

        if parent_idx is None or child_idx is None or percent_idx is None:
            raise CommandError(f"CSV must include parent/child/percent columns. Found: {headers}")
