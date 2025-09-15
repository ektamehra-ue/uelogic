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
        parser.add_argument("--default-unit", default="kWh", help="Unit to use when CSV lacks a unit column/value")
        parser.add_argument("--default-meter-type", default="sub", choices=["fiscal","sub","virtual"],
                            help="Meter type to use when CSV lacks/unknown meter_type")

    def handle(self, *args, **options):
        csv_path = Path(options["csv_path"])
        if not csv_path.exists():
            raise CommandError(f"CSV file not found: {csv_path}")

        dry_run = options["dry_run"]
        delimiter = options["delimiter"]

        
        # Column aliases to be flexible with headings
        aliases = {
        "organization": {"org", "organization", "organisation", "business name"},
        "building": {"building", "site", "sitename"},
        "account": {"account", "tenant", "cost_center", "cost centre", "suite"},
        "identifier": {"identifier", "meter_identifier", "meter point", "meterpointreferenceid", "meter id", "meter", "meter_ref"},
        "external_id": {"external_id", "external id", "source_id", "meter serial", "meterserialnumber"},
        "meter_type": {"meter_type", "type", "meterkind", "expected data source"},  # we'll infer if needed
        "parent_identifier": {"parent_identifier", "parent id", "parent"},          # plus: we’ll also read “Child 1..4” as children->parents
        "unit": {"unit", "uom"},                                                    # will use default if missing
        "is_active": {"is_active", "active", "enabled", "registration status"},
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
        required = ["organization", "building", "identifier", "meter_type", "unit"]
        missing = [k for k in required if not col[k]]
        if missing:
            raise CommandError(f"Missing required column(s): {', '.join(missing)}. Found headers: {headers}")
        
        # Caches to avoid repeated DB lookups
        org_cache = {}
        bld_cache = defaultdict(dict)   # org_id -> {building_name: Building}
        acct_cache = defaultdict(dict)  # org_id -> {account_name: Account}
        meter_buffer = []               # collect first pass
        parent_links = []               # (org_obj, child_meter_identifier, parent_identifier)

        created_counts = dict(org=0, building=0, account=0, meter=0)
        row_count = 0

        
        # First pass: create organization/building/account, buffer meters (parent may not exist yet)
        for r in rows[1:]:
            if not any(r):  # skip completely empty lines
                continue
            row_count += 1

            org_name = norm(r[header_to_idx[col["organization"]]])
            bld_name = norm(r[header_to_idx[col["building"]]])
            acct_name = norm(r[header_to_idx[col["account"]]]) if col["account"] else ""
            identifier = norm(r[header_to_idx[col["identifier"]]])
            external_id = norm(r[header_to_idx[col["external_id"]]]) if col["external_id"] else ""
            meter_type = norm(r[header_to_idx[col["meter_type"]]]).lower()
            parent_identifier = norm(r[header_to_idx[col["parent_identifier"]]]) if col["parent_identifier"] else ""
            unit = norm(r[header_to_idx[col["unit"]]])
            is_active_raw = norm(r[header_to_idx[col["is_active"]]]) if col["is_active"] else "true"
            is_active = (is_active_raw or "true").lower() in {"1", "true", "yes", "y"}

            # get/create org
            org_obj = org_cache.get(org_name)
            if not org_obj and not dry_run:
                org_obj, created = Organization.objects.get_or_create(name=org_name)
                org_cache[org_name] = org_obj
                if created:
                    created_counts["org"] += 1

            # get/create building
            bld_obj = None
            if org_obj:
                bld_obj = bld_cache[org_obj.id].get(bld_name)
                if not bld_obj and not dry_run:
                    bld_obj, created = Building.objects.get_or_create(org=org_obj, name=bld_name)
                    bld_cache[org_obj.id][bld_name] = bld_obj
                    if created:
                        created_counts["building"] += 1

            # get/create account
            acct_obj = None
            if acct_name and org_obj and not dry_run:
                acct_obj = acct_cache[org_obj.id].get(acct_name)
                if not acct_obj:
                    acct_obj, created = Account.objects.get_or_create(org=org_obj, name=acct_name)
                    acct_cache[org_obj.id][acct_name] = acct_obj
                    if created:
                        created_counts["account"] += 1

            # buffer meter (create in second pass so parents can be resolved)
            meter_buffer.append({
                "org_name": org_name,
                "org_obj": org_obj,
                "building_name": bld_name,
                "building_obj": bld_obj,
                "account_name": acct_name,
                "account_obj": acct_obj,
                "identifier": identifier,
                "external_id": external_id or None,
                "meter_type": meter_type,
                "parent_identifier": parent_identifier or None,
                "unit": unit,
                "is_active": is_active,
            })

            if parent_identifier:
                parent_links.append((org_obj, identifier, parent_identifier))

        
        # Second pass: create meters, then wire parents
        with transaction.atomic():
            # create meters
            for m in meter_buffer:
                if dry_run:
                    continue
                org_obj = m["org_obj"]
                if not org_obj:
                    raise CommandError(f"Row with missing org could not be created (identifier={m['identifier']}).")

                # Ensure building exists (safety if bld_obj was None in pass 1 due to dry-run toggling)
                bld_obj = m["building_obj"] or Building.objects.get(org=org_obj, name=m["building_name"])
                acct_obj = m["account_obj"]

                meter, created = Meter.objects.get_or_create(
                    org=org_obj,
                    identifier=m["identifier"],
                    defaults=dict(
                        building=bld_obj,
                        account=acct_obj,
                        external_id=m["external_id"],
                        meter_type=m["meter_type"],
                        unit=m["unit"],
                        is_active=m["is_active"],
                    ),
                )
                if created:
                    created_counts["meter"] += 1

            # wire parents
            for org_obj, child_ident, parent_ident in parent_links:
                if dry_run:
                    continue
                child = Meter.objects.get(org=org_obj, identifier=child_ident)
                parent = Meter.objects.get(org=org_obj, identifier=parent_ident)
                if child.parent_id != parent.id:
                    child.parent = parent
                    child.save(update_fields=["parent"])

        # Summary
        self.stdout.write(self.style.SUCCESS(f"Processed {row_count} rows. Created: "
                                            f"{created_counts['org']} orgs, "
                                            f"{created_counts['building']} buildings, "
                                            f"{created_counts['account']} accounts, "
                                            f"{created_counts['meter']} meters."))






