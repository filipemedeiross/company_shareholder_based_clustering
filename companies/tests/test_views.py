import unittest

from django.test import Client
from django.urls import reverse

from companies.models    import Companies
from companies.constants import COMPANIES_LIST_PAGINATE, \
                                COMPANIES_LIST_CONTEXT


class TestHomeCompaniesView(unittest.TestCase):
    def setUp(self):
        self.client = Client()
        self.url    = reverse('companies:list')

    def test_first_page_returns_paginated_companies_sorted_by_cnpj(self):
        response  = self.client.get(self.url)
        context   = response.context
        companies = context[COMPANIES_LIST_CONTEXT]

        self.assertEqual(response.status_code, 200    )
        self.assertIn   ('page_obj'          , context)
        self.assertTrue (context['is_paginated'])

        cnpjs = [
            company.cnpj
            for company in companies
        ]
        expected_cnpjs = list(
            Companies
            .objects
            .order_by   ('cnpj')
            .values_list('cnpj', flat=True)[:COMPANIES_LIST_PAGINATE]
        )
        self.assertEqual(cnpjs, expected_cnpjs)

    def test_pagination_works_on_second_page(self):
        response = self.client.get(self.url + '?page=2')
        context  = response.context

        self.assertEqual(context['page_obj'].number, 2)

    def test_context_object_name_is_used(self):
        response = self.client.get(self.url)

        self.assertIn(COMPANIES_LIST_CONTEXT, response.context)

    def test_template_used(self):
        response = self.client.get(self.url)

        template_names = [
            t.name
            for t in response.templates
            if t.name
        ]

        self.assertIn('companies/list.html', template_names)
