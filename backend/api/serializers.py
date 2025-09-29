from rest_framework import serializers
from .models import Organization, Building, Account, Meter, VirtualAllocation, Reading

"""
DRF serializer definitions for UELogic.

Purpose:
- Maps Django ORM models (Organization, Building, Account, Meter,
  VirtualAllocation, Reading) to JSON for API responses.
- Handles request payload validation and deserialization into ORM objects.
- Uses ModelSerializer for simplicity, exposing all fields by default.

These serializers are consumed by the viewsets in views.py to translate
between Python objects and REST API input/output.
"""

class OrganizationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Organization
        fields = "__all__"

class BuildingSerializer(serializers.ModelSerializer):
    class Meta:
        model = Building
        fields = "__all__"

class AccountSerializer(serializers.ModelSerializer):
    class Meta:
        model = Account
        fields = "__all__"

class MeterSerializer(serializers.ModelSerializer):
    class Meta:
        model = Meter
        fields = "__all__"

class VirtualAllocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = VirtualAllocation
        fields = "__all__"

class ReadingSerializer(serializers.ModelSerializer):
    class Meta:
        model = Reading
        fields = "__all__"
