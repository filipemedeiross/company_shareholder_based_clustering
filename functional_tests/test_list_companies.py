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

        cls.server = subprocess.Popen([
            "python"        ,
            "manage.py"     ,
            "runserver"     ,
            "127.0.0.1:8000",
        ])
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

        time.sleep(3)
