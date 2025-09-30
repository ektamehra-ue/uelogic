import csv
from pathlib import Path

from django.db import models, transaction
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from api.models import Organization, Building, Meter, Formula


"""
This management command ingests and applies **formula definitions** for virtual meters into the database.

Purpose:
- Loads calculation formulas (e.g., sum, difference, ratio, or custom expressions) 
  that define how one or more meters combine to create derived values.  
- Allows consistent configuration of formulas via CSV uploads.  
- Provides validation for syntax and references to existing meters.
"""

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
    help = "Load Formula rows from formulas CSV. Resolves org via org column, or uniquely by SiteName if org is omitted."


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
        site_idx = idx_of("site", "sitename", "building", "site name")
        kind_idx   = idx_of("meterkind", "kind")                          # required to identify Virtual/Sub
        target_idx = idx_of("target_identifier", "target", "meter",
                            "virtual_identifier", "virtual meter",
                            "meterpointreferenceid", "meter point reference id", "meterpointreference",)
        serial_idx = idx_of("meter_serial_number", "meterserialnumber", "serial", "serialno")
        mpx_idx    = idx_of("meterpointreferenceid", "mpan", "mprn", "meter point reference id")
        expr_idx = idx_of("expression", "expr", "formula")    
        start_idx = idx_of("start", "start_utc", "startutc", "begin", "valid_from")
        end_idx = idx_of("end", "end_utc", "endutc", "valid_to", "stop")

        required_missing = []
        if expr_idx is None:  required_missing.append("expression/formula")
        if start_idx is None: required_missing.append("start/start_utc")
        # require at least one identifier: serial OR MPxN OR internal identifier
        if serial_idx is None and mpx_idx is None and target_idx is None:
            required_missing.append("one of: MeterSerialNumber / MPAN/MPRN / identifier")
        if kind_idx is None:
            required_missing.append("MeterKind")
        if required_missing:
            raise CommandError(f"CSV missing required column(s): {', '.join(required_missing)}. Found: {headers}")


        # Resolve org per row (if provided), else via --org, else error
        default_org = None
        if org_idx is None and org_name_cli:
            try:
                default_org = Organization.objects.get(name=org_name_cli)
            except Organization.DoesNotExist:
                raise CommandError(f"Organization not found: {org_name_cli}")
            
        # initialize counters for reporting
        created = 0
        updated = 0
        total = 0
            
        with csv_path.open(newline="", encoding="utf-8-sig") as f, transaction.atomic():
            reader = csv.reader(f, delimiter=delimiter)
            next(reader, None)  # skip header

            for i, row in enumerate(reader, start=2):  # i = CSV line number (header=1)
                if not row:
                    continue
                total += 1

                # per-row fields
                target_ident = norm(row[target_idx])
                expression = row[expr_idx]  # keep internal spaces as-is
                start_dt = parse_utc(row[start_idx], i)
                end_dt = parse_utc(row[end_idx], i) if end_idx is not None else None
                if end_dt is not None and start_dt >= end_dt:
                    raise CommandError(f"Row {i}: start must be < end (got start={start_dt}, end={end_dt}).")

                # Compute site_name **inside** the loop
                site_name = None
                if site_idx is not None:
                    site_name = norm(row[site_idx]) or None

                # Resolve org (prefer explicit org, else infer via site, else fallback to --org)
                row_org_obj = None
                if org_idx is not None:
                    org_name = norm(row[org_idx])
                    if org_name:
                        try:
                            row_org_obj = Organization.objects.get(name=org_name)
                        except Organization.DoesNotExist:
                            raise CommandError(f"Row {i}: Organization not found: {org_name!r}")

                if row_org_obj is None:
                    if site_name:
                        # Find unique org via building name
                        qs = Building.objects.filter(name=site_name).select_related("org").distinct()
                        count = qs.count()
                        if count == 0:
                            raise CommandError(
                                f"Row {i}: Could not infer organization from site '{site_name}'. "
                                f"Provide an org column or --org."
                            )
                        if count > 1:
                            org_list = ", ".join(sorted({b.org.name for b in qs}))
                            raise CommandError(
                                f"Row {i}: Site '{site_name}' exists in multiple orgs [{org_list}]. "
                                f"Add an org column or use --org to disambiguate."
                            )
                        row_org_obj = qs.first().org
                    else:
                        if default_org is None:
                            raise CommandError(
                                f"Row {i}: No org/sitename for row and no --org provided. "
                                f"Add an org column, a sitename column, or pass --org."
                            )
                        row_org_obj = default_org

                # Meter lookup (prefer scoping to site when possible, fallback to org-only)
                target = None
                if site_name:
                    try:
                        target = Meter.objects.get(
                            org=row_org_obj,
                            building__name=site_name,
                            identifier=target_ident,
                        )
                    except Meter.DoesNotExist:
                        # soft fallback: try org-only lookup
                        try:
                            target = Meter.objects.get(org=row_org_obj, identifier=target_ident)
                            self.stdout.write(
                                self.style.WARNING(
                                    f"Row {i}: site '{site_name}' not found or no meter match; "
                                    f"used org-only lookup for identifier={target_ident!r} in org={row_org_obj.name!r}."
                                )
                            )
                        except Meter.DoesNotExist:
                            raise CommandError(
                                f"Row {i}: target meter not found: identifier={target_ident!r} "
                                f"(site={site_name!r}) in org={row_org_obj.name!r}"
                            )
                        except Meter.MultipleObjectsReturned:
                            raise CommandError(
                                f"Row {i}: multiple meters found for identifier={target_ident!r} in org={row_org_obj.name!r} "
                                f"(site lookup failed for {site_name!r})."
                            )
                else:
                    try:
                        target = Meter.objects.get(org=row_org_obj, identifier=target_ident)
                    except Meter.DoesNotExist:
                        raise CommandError(
                            f"Row {i}: target meter not found: identifier={target_ident!r} in org={row_org_obj.name!r}"
                        )
                    except Meter.MultipleObjectsReturned:
                        raise CommandError(
                            f"Row {i}: multiple meters found for identifier={target_ident!r} in org={row_org_obj.name!r}. "
                            f"Include a SiteName column to disambiguate."
                        )

                if target.meter_type != "virtual":
                    raise CommandError(f"Row {i}: target meter {target_ident!r} is not virtual (found {target.meter_type}).")

                if dry:
                    continue

                obj, created_flag = Formula.objects.update_or_create(
                    target_meter=target,
                    start=start_dt,
                    end=end_dt,
                    defaults={"expression": expression},
                )
                if created_flag:
                    created += 1
                else:
                    updated += 1
