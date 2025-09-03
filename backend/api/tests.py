from django.test import TestCase

class SmokeTests(TestCase):
    def test_health(self):
        from django.urls import reverse
        resp = self.client.get(reverse("health"))
        self.assertEqual(resp.status_code, 200)
