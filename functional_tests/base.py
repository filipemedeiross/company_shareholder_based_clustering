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
