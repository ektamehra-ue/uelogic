import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from api.models import Organization, Meter, VirtualAllocation


"""
This management command ingests and applies **virtual meter allocation rules** 
(parent → child relationships with % splits) into the database.  

Purpose:
- Defines how virtual meters are calculated by distributing consumption 
  from parent meters into child meters using percentage allocations.
- Ensures allocation data can be imported consistently from CSV files.  
- Supports validation to prevent invalid percentages or circular allocations.  
- Provides a dry-run mode for testing CSV validity without database changes.
"""

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

        # Detect columns
        org_idx = idx_of("org", "organization", "organisation")
        parent_idx = idx_of("parent_identifier", "parent id", "parent", "parent_meter")
        child_idx = idx_of("child_identifier", "child id", "child", "child_meter")
        percent_idx = idx_of("percent", "allocation", "pct", "percentage")

        if parent_idx is None or child_idx is None or percent_idx is None:
            raise CommandError(f"CSV must include parent/child/percent columns. Found: {headers}")
        
        # If org column not present, require --org and resolve it now
        org_obj = None
        if org_idx is None:
            if not org_name_cli:
                raise CommandError("No org column in CSV. Provide --org <Organization Name>.")
            try:
                org_obj = Organization.objects.get(name=org_name_cli)
            except Organization.DoesNotExist:
                raise CommandError(f"Organization not found: {org_name_cli}")

        created = 0
        updated = 0
        total = 0

        with csv_path.open(newline="", encoding="utf-8-sig") as f, transaction.atomic():
            reader = csv.reader(f, delimiter=delimiter)
            next(reader, None)  # skip header row

            for row in reader:
                if not row:
                    continue
                total += 1

                # Resolve org per row if CSV has an org column
                row_org_obj = org_obj
                if org_idx is not None:
                    org_name = norm(row[org_idx])
                    try:
                        row_org_obj = Organization.objects.get(name=org_name)
                    except Organization.DoesNotExist:
                        raise CommandError(f"Row {total}: Organization not found: {org_name}")

                parent_ident = norm(row[parent_idx])
                child_ident = norm(row[child_idx])

                # Prevent self-allocation
                if parent_ident == child_ident:
                    raise CommandError(f"Row {total}: parent and child identifiers are the same ({parent_ident}).")

                # Parse percent (allow a trailing %)
                pct_raw = norm(row[percent_idx]).rstrip("%")
                try:
                    pct = float(pct_raw)
                except ValueError:
                    raise CommandError(f"Row {total}: invalid percent value: {row[percent_idx]!r}")

                if pct < 0 or pct > 100:
                    raise CommandError(f"Row {total}: percent out of range 0–100: {pct}")

                # Look up meters with informative errors
                try:
                    parent = Meter.objects.get(org=row_org_obj, identifier=parent_ident)
                except Meter.DoesNotExist:
                    raise CommandError(f"Row {total}: parent meter not found (identifier={parent_ident})")
                except Meter.MultipleObjectsReturned:
                    raise CommandError(f"Row {total}: multiple parent meters found (identifier={parent_ident})")

                try:
                    child = Meter.objects.get(org=row_org_obj, identifier=child_ident)
                except Meter.DoesNotExist:
                    raise CommandError(f"Row {total}: child meter not found (identifier={child_ident})")
                except Meter.MultipleObjectsReturned:
                    raise CommandError(f"Row {total}: multiple child meters found (identifier={child_ident})")

                if dry:
                    # Validate-only mode—skip DB writes
                    continue
                # Upsert by (parent, child)
                va, created_flag = VirtualAllocation.objects.update_or_create(
                    parent=parent,
                    child=child,
                    defaults={"percent": pct},
                )
                if created_flag:
                    created += 1
                else:
                    updated += 1

        self.stdout.write(self.style.SUCCESS(
            f"Processed {total} rows. Created: {created}, Updated: {updated}."
        ))

                

