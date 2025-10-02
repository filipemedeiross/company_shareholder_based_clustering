from django.urls import reverse

from .base import FunctionalTestBase


class DashboardSummarizerTests(FunctionalTestBase):
    base_url = reverse("summarizer:dashboard")

    def test_dashboard_view_loads_in_browser(self):
        self.browser.get(self.base_url)
        self.assertIn("Database Statistics", self.browser.page_source)

    def test_dashboard_template_rendered(self):
        self.browser.get(self.base_url)
        self.assertTrue(self.find_css_element("h2"))
