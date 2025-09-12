from django.db import models

# --- Core / Tenancy ---
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
    Think: the bill-to entity or occupant in a building.
    """
    org = models.ForeignKey(Organization, on_delete=models.CASCADE, related_name="accounts")
    name = models.CharField(max_length=200)

    class Meta:
        unique_together = ("org", "name")

    def __str__(self):
        return f"{self.name} ({self.org.name})"
    

# --- Meters & Allocations ---
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


