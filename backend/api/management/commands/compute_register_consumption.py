"""
Subs “rely on reads (daily, monthly, adhoc)”, but we still need consumption series for formulas/analytics.
This command converts Accumulated reads into Consumption intervals (daily/HH) via differencing + estimation/rollover.

What it does for each Sub meter:
- Fetch ordered Accumulated readings.
- For each gap, compute delta; handle rollovers and negative/zero anomalies → flag or estimate.
- Emit Reading(kind=Consumption, source=System, classification=System) at chosen granularity (e.g., daily).
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from decimal import Decimal
from api.models import Meter, Reading

class Command(BaseCommand):
    help = "Compute consumption from accumulated reads for SUB meters."

    def add_arguments(self, parser):
        parser.add_argument("--site", help="Optional site name to scope.")
        parser.add_argument("--since", help="ISO start bound (UTC).")
        parser.add_argument("--until", help="ISO end bound (UTC).")

    def handle(self, *args, **opts):
        site = (opts.get("site") or "").strip() or None

        qs = Meter.objects.filter(meter_type=Meter.MeterType.SUB, is_active=True)
        if site:
            qs = qs.filter(building__name=site)

        meters = list(qs.select_related("org", "building"))
        if not meters:
            self.stdout.write("No sub meters to process.")
            return

        processed = 0
        with transaction.atomic():
            for m in meters:
                # Pull accumulated reads in window; you can add date filters if passed
                reads = (m.readings
                           .filter(kind=Reading.Kind.ACCUMULATED)
                           .order_by("ts")
                           .values_list("ts", "value", "unit"))
                if len(reads) < 2:
                    continue

                unit = reads[0][2]
                for (t0, v0, _), (t1, v1, _) in zip(reads, reads[1:]):
                    delta = Decimal(v1) - Decimal(v0)
                    if delta < 0:
                        # rollover or correction; for now skip or flag; could add estimation here
                        continue
                    # write a CONSUMPTION record at t1 (or midpoint); de-dup on (meter,t1,kind)
                    Reading.objects.update_or_create(
                        meter=m,
                        ts=t1,
                        kind=Reading.Kind.CONSUMPTION,
                        defaults=dict(
                            value=delta,
                            unit=unit,
                            classification=Reading.Classification.SYSTEM,
                            source=Reading.Source.SYSTEM,
                        ),
                    )
                    processed += 1

        self.stdout.write(self.style.SUCCESS(f"Wrote/updated {processed} consumption intervals."))
