import unittest

from django.test import Client
from django.urls import reverse


class TestDashboardSummarizerView(unittest.TestCase):
    def setUp(self):
        self.client = Client()
        self.url    = reverse('summarizer:dashboard')

    def test_dashboard_returns_status_200(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)

    def test_dashboard_uses_correct_template(self):
        response = self.client.get(self.url)

        template_names = [
            t.name
            for t in response.templates
            if t.name
        ]

        self.assertIn('summarizer/dashboard.html', template_names)

    def test_dashboard_context_contains_expected_key(self):
        response = self.client.get(self.url)
        context  = response.context['stats']

        self.assertIn('companies', context)
        self.assertIn('partners' , context)
        self.assertIn('business' , context)
