from rest_framework import serializers
from .models import Organization, Building, Account, Meter, VirtualAllocation, Reading

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
