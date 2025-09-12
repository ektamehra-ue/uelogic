# backend/api/urls.py
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    health,
    OrganizationViewSet, BuildingViewSet, AccountViewSet,
    MeterViewSet, VirtualAllocationViewSet, ReadingViewSet,
)

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