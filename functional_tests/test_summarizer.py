from django.test import TestCase
from django.urls import reverse

class SummarizerFunctionalTests(TestCase):
    def test_dashboard_view_status_code(self):
        url = reverse('summarizer:dashboard')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)

    def test_dashboard_template_used(self):
        url = reverse('summarizer:dashboard')
        response = self.client.get(url)
        self.assertTemplateUsed(response, 'summarizer/dashboard.html')

    def test_dashboard_context(self):
        url = reverse('summarizer:dashboard')
        response = self.client.get(url)
        self.assertIn('some_expected_context_key', response.context)