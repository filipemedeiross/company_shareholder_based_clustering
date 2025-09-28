import unittest

from django.test import Client
from django.urls import reverse

from companies.models import Companies, \
                             Business , \
                             Partners
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

    def test_pagination_works_with_after(self):
        response_first = self.client.get(self.url)
        page_first     = response_first.context['page_obj']

        response_next = self.client.get(
            f"{self.url}?after={page_first.next_cursor}"
        )
        page_next = response_next.context['page_obj']

        self.assertTrue(page_first.has_next    , "Expected first page to have next page"     )
        self.assertTrue(page_next .has_previous, "Expected second page to have previous page")

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

    def test_search_without_query_returns_200_and_error_message(self):
        response  = self.client.get(self.url)
        context   = response.context
        companies = context[COMPANIES_LIST_CONTEXT]
        messages  = list(context['messages'])

        self.assertEqual(response.status_code, 200 )
        self.assertEqual(len(companies)      , 0   )

        self.assertEqual(len(messages)    , 1 )
        self.assertEqual(messages[0].level, 40)
        self.assertEqual(
            messages[0].message               ,
            'Please fill in the search field.',
        )

    def test_search_with_spaces_only_returns_200_and_no_results(self):
        response  = self.client.get(self.url, {'q': '   '})
        companies = response.context[COMPANIES_LIST_CONTEXT]

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(companies)      , 0  )

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


class TestCompaniesDetailView(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.cnpj = (
            Companies
            .objects
            .order_by('cnpj')
            .first()
            .cnpj
        )

    def setUp(self):
        self.client = Client()
        self.url    = reverse(
            'companies:detail',
            kwargs={'cnpj': self.cnpj}
        )

    def test_detail_view_uses_correct_template(self):
        response = self.client.get(self.url)

        template_names = [
            t.name
            for t in response.templates
            if t.name
        ]

        self.assertEqual(response.status_code   , 200)
        self.assertIn   ('companies/detail.html', template_names)

    def test_context_contains_company_data(self):
        response = self.client.get(self.url)
        context  = response.context

        self.assertIn('company', context)
        self.assertEqual(
            context['company'].cnpj, self.cnpj
        )

    def test_context_contains_business_data(self):
        response = self.client.get(self.url)
        context  = response.context

        self.assertIn('business', context)
        self.assertEqual(
            list(context['business']),
            list(Business.objects.filter(cnpj=self.cnpj))
        )

    def test_context_contains_partners_data(self):
        response = self.client.get(self.url)
        context  = response.context

        self.assertIn('partners', context)
        self.assertEqual(
            list(context['partners']),
            list(Partners.objects.filter(cnpj=self.cnpj))
        )

    def test_nonexistent_cnpj_returns_404(self):
        response = self.client.get(
            reverse(
                'companies:detail',
                kwargs={'cnpj': '99999999'}
            )
        )

        self.assertEqual(response.status_code, 404)
