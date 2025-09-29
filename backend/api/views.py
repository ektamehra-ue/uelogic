from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import viewsets
from django_filters.rest_framework import DjangoFilterBackend

from .models import Organization, Building, Account, Meter, VirtualAllocation, Reading
from .serializers import (
    OrganizationSerializer, BuildingSerializer, AccountSerializer,
    MeterSerializer, VirtualAllocationSerializer, ReadingSerializer
)


"""
Django REST Framework (DRF) view layer for UELogic.

Purpose:
- Exposes CRUD APIs for core domain models: Organization, Building, Account, Meter,
  VirtualAllocation, and Reading.
- Provides a reusable BaseViewSet with filtering enabled for all models.
- Defines the /health endpoint for quick liveness checks.
- Handles serialization ↔ database mapping via corresponding serializers.

This is the main entry point for the REST API, consumed by the React frontend
and other integrations (e.g., ingestion scripts).
"""

@api_view(["GET"])
@permission_classes([AllowAny])
def health(request):
    return Response({"status": "ok"})

# Base viewset with filters enabled
class BaseViewSet(viewsets.ModelViewSet):
    filter_backends = [DjangoFilterBackend]
    filterset_fields = "__all__"

class OrganizationViewSet(BaseViewSet):
    queryset = Organization.objects.all()
    serializer_class = OrganizationSerializer

class BuildingViewSet(BaseViewSet):
    queryset = Building.objects.all()
    serializer_class = BuildingSerializer

class AccountViewSet(BaseViewSet):
    queryset = Account.objects.all()
    serializer_class = AccountSerializer

class MeterViewSet(BaseViewSet):
    queryset = Meter.objects.all()
    serializer_class = MeterSerializer

class VirtualAllocationViewSet(BaseViewSet):
    queryset = VirtualAllocation.objects.all()
    serializer_class = VirtualAllocationSerializer

class ReadingViewSet(BaseViewSet):
    queryset = Reading.objects.all()
    serializer_class = ReadingSerializer