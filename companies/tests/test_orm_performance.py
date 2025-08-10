import time
import unittest

from django.db         import connection
from django.test.utils import CaptureQueriesContext

from companies.models import Companies, \
                             Business , \
                             Partners


class ORMPerformanceTests(unittest.TestCase):
    def _run_and_inspect(
        self             ,
        queryset_callable,
        limit_preview=5  ,
    ):
        with CaptureQueriesContext(connection) as ctx:
            start_time = time.perf_counter()
            results    = list(queryset_callable())
            elapsed_s  = time.perf_counter() - start_time

        print()
        print("--- Query Execution Details ---")
        print(f"SQL: {ctx.captured_queries[0]['sql'] if ctx.captured_queries else 'N/A'}")
        print(f"Execution time: {elapsed_s:.4f} seconds")
        print(f"Total rows: {len(results)}")
        print(f"Sample rows: {results[:limit_preview]}")

        return results


    def test_get_company_by_cnpj(self):
        results = self._run_and_inspect(
            lambda: Companies.objects.filter(cnpj="41273789")
        )
        self.assertTrue(results)


    def test_get_company_by_corporate_name(self):
        results = self._run_and_inspect(
            lambda: Companies.objects.filter(corporate_name="CARLA GARCIA")
        )
        self.assertTrue(results)


    def test_get_cnpjs_by_name_partner(self):
        results = self._run_and_inspect(
        lambda: Companies.objects.filter(partners_fts__name_partner__search="ERY FISCHER")
                                 .values_list("cnpj", flat=True)
        )
        self.assertTrue(results)

    def test_get_partners_by_cnpj(self):
        results = self._run_and_inspect(
            lambda: Partners.objects.filter(cnpj__cnpj="33500091")
                                    .values_list("name_partner", flat=True)
        )
        self.assertTrue(results)
    def test_get_company_by_capital_range(self):
        results = self._run_and_inspect(
            lambda: Companies.objects.filter(capital__gte=1000, capital__lte=10000)
        )
        self.assertTrue(results)




    # ========================
    # Business
    # ========================
    def test_get_business_by_cnpj(self):
        results = self._run_and_inspect(
            lambda: Business.objects.filter(cnpj__cnpj="41273789")
        )
        self.assertTrue(results)

    def test_get_business_by_cnpj_combination(self):
        results = self._run_and_inspect(
            lambda: Business.objects.filter(
                cnpj__cnpj="21368132",
                cnpj_order="0001",
                cnpj_dv="40"
            )
        )
        self.assertTrue(results)

    def test_get_business_by_trade_name(self):
        results = self._run_and_inspect(
            lambda: Business.objects.filter(trade_name__icontains="Alpha")  # ajuste conforme dados reais
        )
        self.assertTrue(results)

