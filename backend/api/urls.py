from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    health,
    OrganizationViewSet, BuildingViewSet, AccountViewSet,
    MeterViewSet, VirtualAllocationViewSet, ReadingViewSet,
)

"""
URL router configuration for UELogic API.

Purpose:
- Registers REST endpoints for all core viewsets (organizations, buildings, accounts,
  meters, allocations, readings).
- Provides a /health route for service monitoring.
- Uses DRF's DefaultRouter to automatically generate CRUD routes.
"""

router = DefaultRouter()
router.register(r"organizations", OrganizationViewSet)
router.register(r"buildings", BuildingViewSet)
router.register(r"accounts", AccountViewSet)
router.register(r"meters", MeterViewSet)
router.register(r"allocations", VirtualAllocationViewSet)
router.register(r"readings", ReadingViewSet)

urlpatterns = [
    path("health", health, name="health"),
    path("", include(router.urls)),
]