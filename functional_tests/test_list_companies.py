import time
import unittest
import subprocess

from selenium import webdriver
from selenium.webdriver.common.by  import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support    import expected_conditions as EC

from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options


class ListCompaniesTests(unittest.TestCase):
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
