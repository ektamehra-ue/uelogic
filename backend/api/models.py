from django.db import models

# --- Core / Tenancy ---
class Organization(models.Model):
    """
    Organisation in this case is Bruntwood.
    It lets you segregate data by org and keeps names unique within each org.
    """
    name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return self.name