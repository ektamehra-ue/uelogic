from django.urls import path, include
from django.contrib import admin
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

urlpatterns = [
    path("api/v1/auth/jwt/create", TokenObtainPairView.as_view(), name="jwt-create"),
    path("api/v1/auth/jwt/refresh", TokenRefreshView.as_view(), name="jwt-refresh"),
    path("admin/", admin.site.urls),
    path("api/v1/", include("api.urls")),  # your DRF routes
]