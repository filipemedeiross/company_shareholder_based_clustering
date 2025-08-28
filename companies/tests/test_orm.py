import time
import unittest

from django.db         import connections
from django.test.utils import CaptureQueriesContext

from companies.models import Companies  , \
                             Partners   , \
                             Business   , \
                             PartnersFts, \
                             BusinessFts


class ORMPerformanceTests(unittest.TestCase):
    def _run_and_inspect(
        self    ,
        queryset,
        limit=3 ,
    ):
        with CaptureQueriesContext(connections['rfb']) as ctx:
            start     = time.perf_counter()
            results   = list(queryset())
            elapsed_s = time.perf_counter() - start

        nrows = len(results)
        limit = limit            \
                if limit < nrows \
                else nrows

        print()
        print("--- Query Execution Details ---"         )
        print(f"SQL: {ctx.captured_queries[0]['sql']}"  )
        print(f"Execution time: {elapsed_s:.4f} seconds")
        print(f"Sample rows ({limit}-{nrows}):")

        for result in results[:limit]:
            print(result)

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


    def test_get_partners_by_cnpj(self):
        results = self._run_and_inspect(
            lambda: Partners.objects.filter(cnpj="33500091")
                                    .values_list("name_partner", flat=True)
        )
        self.assertTrue(results)


    def test_get_cnpjs_by_name_partner(self):
        results = self._run_and_inspect(
        lambda: Partners.objects.filter(name_partner="ERY FISCHER")
                                .values_list("cnpj", flat=True)
        )
        self.assertTrue(results)


    def test_get_business_by_cnpj(self):
        results = self._run_and_inspect(
            lambda: Business.objects.filter(cnpj="41273789")
        )
        self.assertTrue(results)


    def test_get_business_by_cnpj_combination(self):
        results = self._run_and_inspect(
            lambda: Business.objects.filter(
                cnpj="21368132"  ,
                cnpj_order="0001",
                cnpj_dv="40"     ,
            )
        )
        self.assertTrue(results)


    def test_get_business_by_trade_name(self):
        results = self._run_and_inspect(
            lambda: Business.objects.filter(trade_name="PADARIAS MINI")
        )
        self.assertTrue(results)


    def test_search_partners_fts_by_name(self):
        results = self._run_and_inspect(
            lambda: PartnersFts.objects.extra(
                where =["name_partner MATCH %s"],
                params=["CARLA*"]               ,
            )
        )
        self.assertTrue(results)


    def test_search_partners_fts_by_full_name(self):
        results = self._run_and_inspect(
            lambda: PartnersFts.objects.extra(
                where =["name_partner MATCH %s"],
                params=["ERY FISCHER"]          ,
            )
        )
        self.assertTrue(results)


    def test_search_business_fts_by_trade_name(self):
        results = self._run_and_inspect(
            lambda: BusinessFts.objects.extra(
                where =["trade_name MATCH %s"],
                params=["Padaria*"]           ,
            )
        )
        self.assertTrue(results)


    def test_search_business_fts_by_full_trade_name(self):
        results = self._run_and_inspect(
            lambda: BusinessFts.objects.extra(
                where =["trade_name MATCH %s"],
                params=["PADARIAS MINI"]      ,
            )
        )
        self.assertTrue(results)
