import time
import unittest
import subprocess

from selenium import webdriver
from selenium.webdriver.common.by  import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support    import expected_conditions as EC

from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options


class FunctionalTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.server = subprocess.Popen(
            [
                "python"        ,
                "manage.py"     ,
                "runserver"     ,
                "127.0.0.1:8000",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(3)

        service = Service("/snap/bin/geckodriver")
        options = Options()
        options.binary_location = "/snap/firefox/current/usr/lib/firefox/firefox"

        cls.base_url = "http://127.0.0.1:8000"
        cls.browser  = webdriver.Firefox(
            service=service,
            options=options,
        )
        cls.wait     = WebDriverWait(cls.browser, 10)

    @classmethod
    def tearDownClass(cls):
        cls.browser.quit     ()
        cls.server .terminate()

        super().tearDownClass()


class ListCompaniesTests(FunctionalTestBase):
    def test_home_companies_paginated_by_20(self):
        self.browser.get(self.base_url + "/")

        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#company-list")
            )
        )

        rows = self.browser.find_elements(
            By.CSS_SELECTOR             ,
            "#company-list .company-row",
        )
        self.assertEqual(
            len(rows),
            20       ,
            f"Expected 20 companies on the page, but found {len(rows)}"
        )

        paginator = self.browser.find_elements(
            By.CSS_SELECTOR                            ,
            "nav[aria-label='pagination'], .pagination",
        )
        self.assertTrue(
            paginator                    ,
            "No pagination element found",
        )

    def test_pagination_next_click_redirects_to_page_2(self):
        self.browser.get(
            self.base_url + "/"
        )

        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#company-list")
            )
        )

        next_link = self.browser.find_element(
            By.LINK_TEXT,
            "Next"      ,
        )
        next_link.click()

        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#company-list")
            )
        )

        current_url = self.browser.current_url
        self.assertIn(
            "?page=2"  ,
            current_url,
            f"Expected redirect to page 2, got: {current_url}"
        )

    def test_header_companies_click_returns_to_first_page(self):
        self.browser.get(
            self.base_url + "/?page=2"
        )

        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#company-list")
            )
        )

        home_link = self.browser.find_element(
            By.CSS_SELECTOR          ,
            ".top-bar .top-bar-title",
        )
        home_link.click()

        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#company-list")
            )
        )

        current_url = self.browser.current_url
        self.assertTrue(
            current_url.endswith("127.0.0.1:8000/")                ,
            f"Expected to be back on first page: got {current_url}",
        )
        self.assertNotIn(
            "page="    ,
            current_url,
            f"Expected no page param on home",
        )

    def test_layout_and_styling_table_centered(self):
        self.browser.get(
            self.base_url + "/"
        )

        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#company-list .company-table")
            )
        )

        window = self.browser.get_window_size()
        table  = self.browser.find_element(
            By.CSS_SELECTOR, "#company-list .company-table"
        ).rect

        self.assertAlmostEqual(
            table ['x'] + table['width'] / 2,
            window['width']              / 2,
            delta=20                        ,
            msg=f"Table is not approximately centered",
        )

    def test_search_form_placeholder_and_accessibility(self):
        self.browser.get(self.base_url)

        search_input = self.browser.find_element(
            By.CSS_SELECTOR,
            "input[name='q']"
        )

        placeholder = search_input.get_attribute("placeholder")
        self.assertIsNotNone(placeholder, "Search input should have a placeholder")

        aria_label  = search_input.get_attribute("aria-label")
        self.assertIsNotNone(aria_label, "Search input should have aria-label")


class SearchCompaniesTests(FunctionalTestBase):
    def test_search_page_loads_with_search_form(self):
        self.browser.get(
            self.base_url + "/search/?p=00000000"
        )

        search_form = self.browser.find_element(
            By.CSS_SELECTOR,
            ".search-form"
        )
        self.assertTrue(search_form.is_displayed())

        search_input = self.browser.find_element(
            By.CSS_SELECTOR,
            "input[name='q']"
        )
        self.assertTrue(search_input.is_displayed())

        search_button = self.browser.find_element(
            By.CSS_SELECTOR,
            ".search-form button"
        )
        self.assertTrue(search_button.is_displayed())

    def test_search_without_query_returns_404(self):
        self.browser.get(self.base_url + "/search/")
        self.assertIn("404", self.browser.page_source)

    def test_search_results_are_filtered(self):
        self.browser.get(self.base_url + "/")

        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#company-list")
            )
        )

        first_cnpj = self.browser.find_element(
            By.CSS_SELECTOR,
            "#company-list .company-row td:first-child"
        ).text

        search_input = self.browser.find_element(
            By.CSS_SELECTOR,
            "input[name='q']"
        )
        search_button = self.browser.find_element(
            By.CSS_SELECTOR,
            ".search-form button"
        )
        search_input .send_keys(first_cnpj)
        search_button.click()

        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#company-list")
            )
        )
        rows = self.browser.find_elements(
            By.CSS_SELECTOR,
            "#company-list .company-row"
        )
        self.assertGreater(len(rows), 0, "No search results found")

        found_match = False
        for row in rows:
            cnpj_text = row.find_element(
                By.CSS_SELECTOR, "td:first-child"
            ).text

            if cnpj_text.startswith(first_cnpj):
                found_match = True
                break

        self.assertTrue(
            found_match,
            f"CNPJ does not start with {first_cnpj}"
        )

    def test_search_by_corporate_name(self):
        search_term = 'manutencao'

        self.browser.get(
            self.base_url + f"/search/?q={search_term}"
        )

        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#company-list")
            )
        )
        rows = self.browser.find_elements(
            By.CSS_SELECTOR,
            "#company-list .company-row"
        )
        self.assertGreater(len(rows), 0, "No search results found")

        found_match = False
        for row in rows:
            name = row.find_element(
                By.CSS_SELECTOR, "td:nth-child(2)"
            ).text.lower()

            if search_term.lower() in name:
                found_match = True
                break

        self.assertTrue(found_match, f"No company name contains '{search_term}'")
