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

def resolve_target_meter(*, site_name, serial, mpx, ident, strict_site):
        """
        Returns (meter, via). Tries serial -> MPxN -> identifier.
        Uses SiteName to disambiguate when provided. No org required.
        """
        from api.models import Meter  # local import to avoid circulars if any
        base = Meter.objects.all()

        # 1) serial
        if serial:
            q = base.filter(external_id=serial)
            if site_name:
                q_site = q.filter(building__name=site_name)
                if q_site.count() == 1:
                    return q_site.get(), f"serial={serial!r} @ {site_name}"
                if strict_site:
                    from django.core.management.base import CommandError
                    raise CommandError(f"Serial {serial!r} not unique or not found at site={site_name!r}.")
            if q.count() == 1:
                return q.get(), f"serial={serial!r}"
            if q.count() > 1:
                from django.core.management.base import CommandError
                raise CommandError(f"Serial {serial!r} matches {q.count()} meters; add SiteName to disambiguate.")

        # 2) MPxN
        if mpx:
            q = base.filter(external_id=mpx)
            if site_name:
                q_site = q.filter(building__name=site_name)
                if q_site.count() == 1:
                    return q_site.get(), f"mpx={mpx!r} @ {site_name}"
                if strict_site:
                    from django.core.management.base import CommandError
                    raise CommandError(f"MPxN {mpx!r} not unique or not found at site={site_name!r}.")
            if q.count() == 1:
                return q.get(), f"mpx={mpx!r}"
            if q.count() > 1:
                from django.core.management.base import CommandError
                raise CommandError(f"MPxN {mpx!r} matches {q.count()} meters; add SiteName to disambiguate.")

        # 3) identifier
        if ident:
            q = base.filter(identifier=ident)
            if site_name:
                q = q.filter(building__name=site_name)
            try:
                return q.get(), f"identifier={ident!r}{' @ '+site_name if site_name else ''}"
            except Meter.DoesNotExist:
                pass
            except Meter.MultipleObjectsReturned:
                from django.core.management.base import CommandError
                raise CommandError(f"Identifier {ident!r} is ambiguous; add SiteName.")

        from django.core.management.base import CommandError
        raise CommandError("Could not uniquely resolve a meter via serial/MPxN/identifier.")



class Command(BaseCommand):
    help = "Load Formula rows from formulas CSV. Resolves org via org column, or uniquely by SiteName if org is omitted."

    def add_arguments(self, parser):
        parser.add_argument("--strict-site", action="store_true",
            help="Error if a number (serial/MPxN) matches multiple meters and SiteName is missing or not unique.")
        parser.add_argument("csv_path", type=str, help="Path to CSV file.")
        parser.add_argument("--org", type=str, default=None, help="Organization name (used only if CSV lacks an org column;if both are present, CSV value takes precedence)")
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

        strict_site = opts["strict_site"]
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
        target_idx = idx_of("target_identifier", "target", "meter", "identifier")
        serial_idx = idx_of("meter_serial_number", "meterserialnumber", "serial", "serialno")
        mpx_idx    = idx_of("meterpointreferenceid", "mpan", "mprn", "meter point reference id")
        expr_idx = idx_of("expression", "expr", "formula")    
        start_idx = idx_of("start", "start_utc", "startutc", "begin", "valid_from")
        end_idx = idx_of("end", "end_utc", "endutc", "valid_to", "stop")

        required_missing = []
        if expr_idx is None:  required_missing.append("expression/formula")
        if start_idx is None: required_missing.append("start/start_utc")
        if kind_idx is None:  required_missing.append("MeterKind")
        if serial_idx is None and mpx_idx is None and target_idx is None:
            required_missing.append("one of: MeterSerialNumber / MPAN|MPRN / identifier")
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

            for i, row in enumerate(reader, start=2):
                if not row:
                    continue
                total += 1

                # Site (optional)
                site_name = norm(row[site_idx]) or None if site_idx is not None else None

                # Meter kind (we support Virtual and Sub targets)
                kind = (norm(row[kind_idx]) or "").lower()
                if kind not in {"virtual", "sub"}:
                    # skip fiscal/others
                    continue

                # Expression + time window
                expression = row[expr_idx]
                start_dt = parse_utc(row[start_idx], i)
                end_dt = parse_utc(row[end_idx], i) if end_idx is not None else None
                if end_dt is not None and start_dt >= end_dt:
                    raise CommandError(f"Row {i}: start must be < end (got start={start_dt}, end={end_dt}).")

                # Numbers-first resolution inputs
                serial = norm(row[serial_idx]) if serial_idx is not None else None
                mpx    = norm(row[mpx_idx])    if mpx_idx    is not None else None
                ident  = norm(row[target_idx]) if target_idx is not None else None

                # Resolve target meter by serial -> MPxN -> identifier
                try:
                    target, via = resolve_target_meter(
                        site_name=site_name,
                        serial=serial,
                        mpx=mpx,
                        ident=ident,
                        strict_site=strict_site,
                    )
                except CommandError as e:
                    raise CommandError(f"Row {i}: {e}")

                # Enforce intended type
                expected_type = Meter.MeterType.VIRTUAL if kind == "virtual" else Meter.MeterType.SUB
                if target.meter_type != expected_type:
                    raise CommandError(
                        f"Row {i}: MeterKind={kind} but resolved meter is {target.meter_type} (via {via})."
                    )

                if dry:
                    continue

                # Upsert by (target_meter, start, end)
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

        # Summary
        self.stdout.write(self.style.SUCCESS(
        f"Processed {total} rows. Created: {created}, Updated: {updated}."
        ))
