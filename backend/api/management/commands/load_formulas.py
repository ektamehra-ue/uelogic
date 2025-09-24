import csv
from pathlib import Path

from django.db import models, transaction
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from api.models import Organization, Meter, Formula

def norm(s):
    return (s or "").strip()


def parse_utc(dt_str, row_num):
    """
    Accepts common ISO8601 formats (e.g., 2025-07-01T00:00:00Z, 2025-07-01 00:00:00, etc.)
    Returns timezone-aware UTC datetimes. Raises CommandError on failure.
    """
    raw = norm(dt_str)
    if not raw:
        return None
    
    # Normalise trailing 'Z' (UTC) so parse_datetime sees timezone
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    dt = parse_datetime(raw)
    if dt is None:
        raise CommandError(f"Row {row_num}: could not parse datetime: {dt_str!r}")

    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt

class Command(BaseCommand):
    help = (
        "Load Formula rows from a CSV file. "
        "Expected columns: org (optional), target_identifier, expression, start_utc, end_utc (optional)."
    )

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str, help="Path to CSV file.")
        parser.add_argument("--org", type=str, default=None, help="Organization name (used only if CSV lacks an org column; if both are present, CSV value takes precedence)")
        parser.add_argument("--dry-run", action="store_true", help="Validate only; no DB write")
        parser.add_argument("--delimiter", default=",", help="CSV delimiter (default ,)")
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Delete and re-insert formulas where organization, target identifier, start_utc, and end_utc match existing records."
        )


    def handle(self, *args, **opts):
        def handle(self, *args, **opts):
        csv_path = Path(opts["csv_path"]).resolve()
        if not csv_path.exists():
            raise CommandError(f"CSV not found: {csv_path}")

        org_name_cli = opts["org"]
        dry = opts["dry_run"]
        delimiter = opts["delimiter"]
        replace = opts["replace"]

        # Read header safely
        with csv_path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f, delimiter=delimiter)
            try:
                headers = next(reader)
            except StopIteration:
                self.stdout.write(self.style.WARNING("Empty CSV"))
                return
        headers_lower = [h.strip().lower() for h in headers]

        def idx_of(*cands):
            for c in cands:
                c_low = c.strip().lower()
                if c_low in headers_lower:
                    return headers_lower.index(c_low)
            return None
        
        # Explected flixible columns
        org_idx = idx_of("org", "organization", "organisation")
        target_idx = idx_of("target_identifier", "target", "meter", "virtual_identifier", "virtual meter")
        expr_idx = idx_of("expression", "expr", "formula")
        start_idx = idx_of("start", "start_utc", "begin", "valid_from")
        end_idx = idx_of("end", "end_utc", "valid_to", "stop")

        missing = [name for name, idx in {
            "target_identifier": target_idx,
            "expression": expr_idx,
            "start": start_idx,
        }.items() if idx is None]
        if missing:
            raise CommandError(f"CSV missing required column(s): {', '.join(missing)}. Found: {headers}")

        # Resolve org if column absent
        default_org = None
        if org_idx is None:
            if not org_name_cli:
                raise CommandError("No org column in CSV. Provide --org <Organization Name>.")
            try:
                default_org = Organization.objects.get(name=org_name_cli)
            except Organization.DoesNotExist:
                raise CommandError(f"Organization not found: {org_name_cli}")

        created = 0
        updated = 0
        total = 0

        with csv_path.open(newline="", encoding="utf-8-sig") as f, transaction.atomic():
            reader = csv.reader(f, delimiter=delimiter)
            next(reader, None)  # skip header

            for i, row in enumerate(reader, start=2):  # start=2 for human line numbers (header=1)
                if not row:
                    continue
                total += 1

                # org per-row
                org_obj = default_org
                if org_idx is not None:
                    org_name = norm(row[org_idx])
                    try:
                        org_obj = Organization.objects.get(name=org_name)
                    except Organization.DoesNotExist:
                        raise CommandError(f"Row {i}: Organization not found: {org_name!r}")

                target_ident = norm(row[target_idx])
                expression = row[expr_idx]  # keep as-is (don’t strip internal spaces)
                start_dt = parse_utc(row[start_idx], i)
                end_dt = parse_utc(row[end_idx], i) if end_idx is not None else None

                if end_dt is not None and start_dt >= end_dt:
                    raise CommandError(f"Row {i}: start must be < end (got start={start_dt}, end={end_dt}).")
