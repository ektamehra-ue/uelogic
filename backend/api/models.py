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