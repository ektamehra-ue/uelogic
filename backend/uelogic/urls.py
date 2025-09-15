from django.urls import path, include
from django.contrib import admin
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

urlpatterns = [
    path("api/auth/jwt/create", TokenObtainPairView.as_view(), name="jwt-create"),
    path("api/auth/jwt/refresh", TokenRefreshView.as_view(), name="jwt-refresh"),
    path("admin/", admin.site.urls),
    path("api/", include("api.urls")),  # your DRF routes
    path("api-auth/", include("rest_framework.urls"))  # enables Login in browsable API
]