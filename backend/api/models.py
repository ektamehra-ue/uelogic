from django.db import models

"""
Core data model for UELogic platform.

Purpose:
- Defines database schema for Organizations, Buildings, Accounts, Meters,
  Allocations, Formulas, and Readings.
- Encodes business rules such as:
    • Organizations contain buildings and accounts.
    • Meters can be fiscal, sub, or virtual, and may belong to accounts/buildings.
    • VirtualAllocations distribute parent meter usage to child meters by %.
    • Formulas define derived calculations for virtual meters over time windows.
    • Readings store time series data with provenance and semantics.
- Provides uniqueness constraints, foreign key relationships, and indexing
  for efficient lookups.

This schema underpins ingestion, allocation, calculation, and reporting logic
for the platform.
"""

# Core / Tenancy
class Organization(models.Model):
    """
    Organization in this case is Bruntwood.
    It lets you segregate data by org and keeps names unique within each org.
    """
    name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return self.name
    
class Building(models.Model):
    """
    A physical site that belongs to an Organization.
    """
    org = models.ForeignKey(Organization, on_delete=models.CASCADE, related_name="buildings")
    name = models.CharField(max_length=200)

    class Meta:
        unique_together = ("org", "name")

    def __str__(self):
        return f"{self.name} ({self.org.name})"


class Account(models.Model):
    """
    Tenant cost center under an Organization.
    The bill to the entity or occupant in a building.
    """
    org = models.ForeignKey(Organization, on_delete=models.CASCADE, related_name="accounts")
    name = models.CharField(max_length=200)

    class Meta:
        unique_together = ("org", "name")

    def __str__(self):
        return f"{self.name} ({self.org.name})"
    

# Meters & Allocations
class Meter(models.Model):
    """
    A meter can be fiscal (main/billing), sub (downstream), or virtual (computed).
    - account -> ForeignKey to Account. Some meters are at building level only.
    - parent   -> Link for submeter trees (e.g., a submeter's parent is a fiscal meter).
    - external_id -> Use to map to vendor/CSV/system IDs.
    - identifier  -> Our internal unique code.
    """
    class MeterType(models.TextChoices):
        FISCAL = "fiscal", "Fiscal (Billing/Main)"
        SUB = "sub", "Submeter"
        VIRTUAL = "virtual", "Virtual (Computed)"

    org = models.ForeignKey(Organization, on_delete=models.CASCADE, related_name="meters")
    building = models.ForeignKey(Building, on_delete=models.CASCADE, related_name="meters")
    account = models.ForeignKey(Account, null=True, blank=True,
                                on_delete=models.SET_NULL, related_name="meters")  # optional
    identifier = models.CharField(max_length=200)  # unique per org
    external_id = models.CharField(max_length=200, null=True, blank=True)
    meter_type = models.CharField(max_length=10, choices=MeterType.choices, default=MeterType.SUB)
    parent = models.ForeignKey("self", null=True, blank=True,
                               on_delete=models.SET_NULL, related_name="children")
    unit = models.CharField(max_length=32, help_text="e.g. kWh, m³, kW, °C")
    is_active = models.BooleanField(default=True)

    class Meta:
        unique_together = ("org", "identifier")
        indexes = [
            models.Index(fields=["org", "identifier"]),
            models.Index(fields=["building"]),
            models.Index(fields=["account"]),
            models.Index(fields=["meter_type"]),
        ]

    def __str__(self):
        return f"{self.identifier} [{self.meter_type}]"


class VirtualAllocation(models.Model):
    """
    Allocation graph for virtual meters: distribute a PARENT meter into CHILD meters by percent.
    Note: Both parent and child are meters. Typically parent is fiscal; child is sub.
    """
    parent = models.ForeignKey(Meter, on_delete=models.CASCADE, related_name="allocations_out")
    child = models.ForeignKey(Meter, on_delete=models.CASCADE, related_name="allocations_in")
    percent = models.DecimalField(max_digits=7, decimal_places=4, help_text="0.0–100.0")

    class Meta:
        unique_together = ("parent", "child")

    def __str__(self):
        return f"{self.parent.identifier} → {self.child.identifier} ({self.percent}%)"


# Virtual Formulas
class Formula(models.Model):
    """
    A time-bounded formula that defines how to compute readings for a *virtual* meter.
    Later, a compute_virtuals command will evaluate these expressions over source meter readings
    and insert calculated readings for the target virtual meter.
    """
    target_meter = models.ForeignKey(
        Meter,
        on_delete=models.CASCADE,
        related_name="formulas",
        help_text="Must be a virtual meter.",
        limit_choices_to={"meter_type": Meter.MeterType.VIRTUAL},
    )
    expression = models.TextField(help_text="Expression DSL, e.g. 'FISCAL - (SUB1 + 0.5*SUB2)'.")
    start = models.DateTimeField(help_text="Inclusive UTC start.")
    end = models.DateTimeField(null=True, blank=True, help_text="Exclusive UTC end; null means open-ended.")

    class Meta:
        indexes = [
            models.Index(fields=["target_meter", "start"]),
            models.Index(fields=["target_meter", "end"]),
        ]
        unique_together = ("target_meter", "start", "end")  # natural upsert key

    def clean(self):
        # Ensure sensible window
        if self.end is not None and self.start >= self.end:
            from django.core.exceptions import ValidationError
            raise ValidationError("end must be greater than start (or null for open-ended).")

    def __str__(self):
        end_str = self.end.isoformat() if self.end else "open"
        return f"Formula({self.target_meter.identifier}: {self.start.isoformat()} → {end_str})"


# Readings 
class Reading(models.Model):
    """
    Time series values for a meter.
    - unique_together (meter, ts) -> ensures you don’t double-ingest the same timestamp for a meter.
    - classification/kind/source -> provenance + semantics.
    """
    class Classification(models.TextChoices):
        ACTUAL = "Actual", "Actual"
        ESTIMATED = "Estimated", "Estimated"
        MANUAL = "Manual", "Manual"
        SYSTEM = "System", "System"

    class Source(models.TextChoices):
        API = "API", "API"
        CSV = "CSV", "CSV"
        MANUAL = "Manual", "Manual"

    class Kind(models.TextChoices):
        ACCUMULATED = "Accumulated", "Accumulated"  # cumulative register (e.g., kWh totalizer)
        CONSUMPTION = "Consumption", "Consumption"  # interval usage (e.g., kWh during period)

    meter = models.ForeignKey(Meter, on_delete=models.CASCADE, related_name="readings")
    ts = models.DateTimeField(db_index=True)  # timestamp in UTC ideally
    value = models.DecimalField(max_digits=18, decimal_places=6)
    unit = models.CharField(max_length=32)
    classification = models.CharField(max_length=10, choices=Classification.choices,
                                      default=Classification.ACTUAL)
    source = models.CharField(max_length=10, choices=Source.choices, default=Source.CSV)
    kind = models.CharField(max_length=12, choices=Kind.choices, default=Kind.CONSUMPTION)

    class Meta:
        unique_together = ("meter", "ts")
        indexes = [
            models.Index(fields=["meter", "ts"]),
        ]

    def __str__(self):
        return f"{self.meter.identifier} @ {self.ts} = {self.value} {self.unit}"