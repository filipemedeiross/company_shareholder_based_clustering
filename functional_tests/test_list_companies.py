import sys
import time
import unittest
import subprocess

from selenium import webdriver
from selenium.webdriver.common.by  import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support    import expected_conditions as EC


class FunctionalTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.server = subprocess.Popen(
            [
                cls.get_python(),
                "manage.py"     ,
                "runserver"     ,
                "127.0.0.1:8000",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(1)

        cls.base_url = "http://127.0.0.1:8000"
        cls.browser  = webdriver.Firefox()

        cls.wait = WebDriverWait(cls.browser, 10)

    @classmethod
    def tearDownClass(cls):
        cls.browser.quit     ()
        cls.server .terminate()

        super().tearDownClass()

    def open_home(self, page="/"):
        self.browser.get(self.base_url + page)

    def find_css_element (self, value):
        return self.browser.find_element (By.CSS_SELECTOR, value)

    def find_css_elements(self, value):
        return self.browser.find_elements(By.CSS_SELECTOR, value)

    def get_search_elements(self):
        return self.find_css_element("input[name='q']"    ), \
               self.find_css_element(".search-form button")

    def wait_for_company_list(self):
        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#company-list")
            )
        )

    @staticmethod
    def get_python():
        if (hasattr(sys, "real_prefix")) or \
           (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix):
            return sys.executable

        return "python"


class ListCompaniesTests(FunctionalTestBase):
    def test_home_companies_paginated_by_20(self):
        self.open_home()
        self.wait_for_company_list()

        rows      = self.find_css_elements("#company-list .company-row")
        paginator = self.find_css_elements("nav[aria-label='pagination'], .pagination")

        self.assertEqual(len(rows), 20, "Expected 20 companies on the page")
        self.assertTrue (paginator,     "No pagination element found"      )

    def test_pagination_next_click_redirects_with_after(self):
        self.open_home()
        self.wait_for_company_list()

        next_link = self.browser.find_element (By.LINK_TEXT, "Next")
        next_href = next_link   .get_attribute("href")
        next_link.click()

        self.wait_for_company_list()

        self.assertIn("after=", next_href)
        self.assertIn("after=", self.browser.current_url)

    def test_header_companies_click_returns_to_first_page(self):
        self.open_home()
        self.wait_for_company_list()

        next_link = self.browser.find_element(By.LINK_TEXT, "Next")
        next_link.click()

        self.wait_for_company_list()

        self.find_css_element(
            ".top-bar .top-bar-title",
        ).click()

        self.wait_for_company_list()

        current_url = self.browser.current_url
        self.assertTrue(
            current_url.endswith("127.0.0.1:8000/")                ,
            f"Expected to be back on first page: got {current_url}",
        )
        self.assertNotIn(
            "before="  ,
            current_url,
            f"Expected no page param on home",
        )

    def test_layout_and_styling_table_centered(self):
        self.open_home()
        self.wait_for_company_list()

        window = self.browser.get_window_size()
        table  = self.find_css_element(
            "#company-list .company-table"
        ).rect

        self.assertAlmostEqual(
            table ['x'] + table['width'] / 2,
            window['width']              / 2,
            delta=20                        ,
            msg=f"Table is not approximately centered",
        )

    def test_search_form_placeholder_and_accessibility(self):
        self.open_home()

        search_input = self.find_css_element ("input[name='q']")
        placeholder  = search_input.get_attribute("placeholder")
        aria_label   = search_input.get_attribute("aria-label" )

        self.assertIsNotNone(placeholder, "Search input should have a placeholder")
        self.assertIsNotNone(aria_label , "Search input should have aria-label"   )


class SearchCompaniesTests(FunctionalTestBase):
    def test_search_page_loads_with_search_form(self):
        self.open_home("/search/?q=00000000")
        self.wait_for_company_list()

        search_form   = self.find_css_element(".search-form"   )
        search_input  = self.find_css_element("input[name='q']")
        search_button = self.find_css_element(".search-form button")

        self.assertTrue(search_form.is_displayed  ())
        self.assertTrue(search_input.is_displayed ())
        self.assertTrue(search_button.is_displayed())

    def test_empty_search_shows_error_message_and_no_results(self):
        self.open_home("/search/?q=")
        self.wait_for_company_list()

        self.assertIn("Please fill in the search field.", self.browser.page_source)
        self.assertIn("No companies found."             , self.browser.page_source)

    def test_search_input_is_required_on_home(self):
        self.open_home()

        search_input = self.find_css_element("input[name='q']")
        search_input.clear()

        self.find_css_element(
            ".search-form button"
        ).click()

        current_url = self.browser.current_url

        self.assertFalse(
            search_input.get_property("validity")["valid"],
            "Expected input to be invalid when empty"
        )
        self.assertTrue(
            current_url.endswith("/"),
            f"Expected to remain on home page, but got redirected to {current_url}"
        )

    def test_search_results_are_filtered(self):
        self.open_home()
        self.wait_for_company_list()

        first_cnpj = self.find_css_element(
            "#company-list .company-row td:first-child"
        ).text

        search_input = self.find_css_element("input[name='q']")
        search_input.send_keys(first_cnpj)

        self.find_css_element(
            ".search-form button"
        ).click()

        self.wait_for_company_list()

        rows = self.find_css_elements("#company-list .company-row")

        found_match = False
        for row in rows:
            cnpj_text = row.find_element(
                By.CSS_SELECTOR, "td:first-child"
            ).text

            if cnpj_text.startswith(first_cnpj):
                found_match = True
                break

        self.assertGreater(len(rows), 0, "No search results found")
        self.assertTrue   (found_match , f"CNPJ does not start with {first_cnpj}")

    def test_search_by_corporate_name(self):
        search_term = 'manutencao'

        self.open_home(f"/search/?q={search_term}")
        self.wait_for_company_list()

        rows = self.find_css_elements("#company-list .company-row")

        found_match = False
        for row in rows:
            name = row.find_element(
                By.CSS_SELECTOR, "td:nth-child(2)"
            ).text.lower()

            if search_term.lower() in name:
                found_match = True
                break

        self.assertGreater(len(rows), 0, "No search results found")
        self.assertTrue   (found_match , f"No company name contains '{search_term}'")
