import unittest

from django.test import Client
from django.urls import reverse

from companies.models import Companies


class TestHomeCompaniesView(unittest.TestCase):
    def setUp(self):
        self.client = Client()

    def test_first_page_returns_20_companies_sorted_by_cnpj(self):
        response = self.client.get(reverse('companies:list'))

        context   = response.context
        companies = context['companies']

        self.assertEqual(response.status_code, 200)
        self.assertIn   ('companies', context)
        self.assertIn   ('page_obj' , context)
        self.assertTrue (context['is_paginated'])

        cnpjs = [
            company.cnpj
            for company in companies
        ]

        self.assertEqual(len(cnpjs)   , 20   )
        self.assertEqual(sorted(cnpjs), cnpjs)

        expected_cnpjs = list(
            Companies
            .objects
            .order_by('cnpj')
            .values_list('cnpj', flat=True)[:20]
        )
        self.assertEqual(cnpjs, expected_cnpjs)
