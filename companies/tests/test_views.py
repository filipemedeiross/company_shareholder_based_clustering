import unittest

from django.test import Client
from django.urls import reverse

from companies.models    import Companies
from companies.constants import COMPANIES_LIST_PAGINATE, \
                                COMPANIES_LIST_CONTEXT


class TestCompaniesListView(unittest.TestCase):
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


class TestCompaniesSearchView(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.first_cnpj = (
            Companies
            .objects
            .order_by('cnpj')
            .values_list('cnpj', flat=True)
            .first()
        )
        cls.any_name = (
            Companies
            .objects
            .exclude(corporate_name__isnull=True)
            .exclude(corporate_name='')
            .values_list('corporate_name', flat=True)
            .first()
        )

    def setUp(self):
        self.client = Client()
        self.url    = reverse('companies:search')

    def test_search_without_query_returns_404(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 404)

    def test_search_with_empty_query_returns_404(self):
        response = self.client.get(self.url, {'q': ''})
        self.assertEqual(response.status_code, 404)

    def test_search_with_valid_query_returns_200(self):
        response = self.client.get(self.url, {'q': self.first_cnpj})
        self.assertEqual(response.status_code, 200)

    def test_search_filters_by_cnpj_startswith(self):
        response  = self.client.get(self.url, {'q': self.first_cnpj})
        companies = response.context[COMPANIES_LIST_CONTEXT]

        self.assertTrue(
            any(
                company.cnpj.startswith(self.first_cnpj)
                for company in companies
            )
        )

    def test_search_filters_by_corporate_name_icontains(self):
        search_term = self.any_name[:5]

        response  = self.client.get(self.url, {'q': search_term})
        companies = response.context[COMPANIES_LIST_CONTEXT]

        self.assertTrue(
            any(
                search_term.lower() in company.corporate_name.lower()
                for company in companies
            )
        )

    def test_search_case_insensitive_for_corporate_name(self):
        search_term = self.any_name[:5]

        response_upper = self.client.get(self.url, {'q': search_term.upper()})
        response_lower = self.client.get(self.url, {'q': search_term.lower()})

        companies_upper = response_upper.context[COMPANIES_LIST_CONTEXT]
        companies_lower = response_lower.context[COMPANIES_LIST_CONTEXT]

        self.assertEqual(len(companies_upper), len(companies_lower))

    def test_search_preserves_query_in_pagination(self):
        search_term = self.any_name[:5]

        page2     = self.client.get(self.url, {'q': search_term, 'page': 2})
        companies = page2.context[COMPANIES_LIST_CONTEXT]

        self.assertTrue(
            any(
                search_term.lower() in company.corporate_name.lower()
                for company in companies
            )
        )

    def test_search_inherits_pagination_from_parent(self):
        response = self.client.get(self.url, {'q': self.first_cnpj})

        self.assertIn('page_obj'    , response.context)
        self.assertIn('is_paginated', response.context)

    def test_search_uses_correct_template(self):
        response = self.client.get(self.url, {'q': self.first_cnpj})

        template_names = [
            t.name
            for t in response.templates
            if t.name
        ]

        self.assertIn('companies/search.html', template_names)
