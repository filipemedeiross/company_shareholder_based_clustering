from django.urls import reverse

from .base import FunctionalTestBase


class DashboardSummarizerTests(FunctionalTestBase):
    summarizer_url = reverse("summarizer:dashboard")

    def test_dashboard_view_loads_in_browser(self):
        self.open_home(self.summarizer_url)
        self.assertIn("Database Statistics", self.browser.page_source)

    def test_dashboard_template_rendered(self):
        self.open_home(self.summarizer_url)
        self.assertTrue(self.find_css_element("h2"))
