from django.urls import path, include
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

urlpatterns = [
    path("api/v1/auth/jwt/create", TokenObtainPairView.as_view(), name="jwt-create"),
    path("api/v1/auth/jwt/refresh", TokenRefreshView.as_view(), name="jwt-refresh"),
    path("api/v1/", include("api.urls")),  # your DRF routes
]