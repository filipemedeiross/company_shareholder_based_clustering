import csv
import random
import unittest
import pandas as pd

from pathlib     import Path
from fastparquet import ParquetFile


class TestParquet(unittest.TestCase):
    ROOT_DIR = Path(__file__).resolve().parent.parent

    PARTNERS_PARQUET  = ROOT_DIR / 'data/parquet/partners.parquet'
    COMPANIES_PARQUET = ROOT_DIR / 'data/parquet/companies.parquet'
    BUSINESS_PARQUET  = ROOT_DIR / 'data/parquet/business.parquet'


    def get_sample_from_parquet(self, filename, n=3):
        samples = []

        with open(filename, mode='rb') as f:
            pf   = ParquetFile(f)
            idxs = sorted(random.sample(range(pf.count()), n))

            current_row = 0
            for rg in pf.iter_row_groups():
                num_rows = len(rg)

                local_indices = [
                    i - current_row
                    for i in idxs
                    if current_row <= i < current_row + num_rows
                ]

                if local_indices:
                    samples.append(rg.iloc[local_indices])

                current_row += num_rows
                if current_row > idxs[-1]:
                    break

        return pd.concat(samples, ignore_index=True)


    def find_row_partners(
        self    ,
        files   ,
        cols    ,
        cnpj_col,
        name_col,
        target  ,
    ):
        for csv_path in files:
            with open(csv_path, encoding="latin-1", newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')

                for row in reader:
                    if (
                        int(row[cnpj_col]) == target.cnpj and
                            row[name_col]  == target.name_partner
                    ):
                        return [row[idx] for idx in cols]

        return None


    def find_row_business(
        self     ,
        files    ,
        cols     ,
        cnpj_col ,
        order_col,
        dv_col   ,
        target   ,
    ):
        for csv_path in files:
            with open(csv_path, encoding="latin-1", newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')

                for row in reader:
                    if (
                        int(row[cnpj_col ]) == target.cnpj       and
                        int(row[order_col]) == target.cnpj_order and
                        int(row[dv_col   ]) == target.cnpj_dv
                    ):
                        return [row[idx] for idx in cols]

        return None


    def test_partners_parquet_contains_csv_data(self):
        print()
        print("▶️ Starting test: test_partners_parquet_contains_csv_data...")
        print()

        CNPJ_COL, NAME_COL, _ = SOCIOS_COLS = [0, 2, 5]
        SOCIOS_PATH = [
            self.ROOT_DIR / f'data/csv/socios/socios{i}.csv'
            for i in range(10)
        ]

        sample = self.get_sample_from_parquet(self.PARTNERS_PARQUET)

        for _, row in sample.iterrows():
            found = self.find_row_partners(
                SOCIOS_PATH,
                SOCIOS_COLS,
                CNPJ_COL   ,
                NAME_COL   ,
                row        ,
            )

            cnpj, name, start_date = row
            with self.subTest(
                cnpj=cnpj       ,
                partner=name    ,
                start=start_date,
            ):
                if found:
                    print(
                        f"✅ Found match:\n"
                        f"Parquet:     {cnpj}, {name}, {start_date}\n"
                        f"CSV Match:   {found[0]}, {found[1]}, {found[2]}"
                    )
                else:
                    self.fail(
                        f"❌ CNPJ {cnpj} from parquet not found in any socios[0-9].csv file."
                    )


    def test_business_parquet_contains_csv_data(self):
        print()
        print("▶️ Starting test: test_business_parquet_contains_csv_data")
        print()

        BASIC_COL = 0
        ORDER_COL = 1
        DV_COL    = 2
        ESTABELECIMENTOS_COLS = [0, 1, 2, 3, 4, 6, 10, 18]

        ESTABELECIMENTOS_PATH = [
            self.ROOT_DIR / f'data/csv/estabelecimentos/estabelecimentos{i}.csv'
            for i in range(10)
        ]

        sample = self.get_sample_from_parquet(self.BUSINESS_PARQUET)

        for _, row in sample.iterrows():
            found = self.find_row_business(
                ESTABELECIMENTOS_PATH,
                ESTABELECIMENTOS_COLS,
                BASIC_COL            ,
                ORDER_COL            ,
                DV_COL               ,
                row                  ,
            )

            cnpj, order, dv, branch, trade_name, closing, opening, cep = row
            with self.subTest(
                cnpj=cnpj      ,
                order=order    ,
                dv=dv          ,
                branch=branch  ,
                name=trade_name,
                start=opening  ,
                end=closing    ,
                cep=cep        ,
            ):
                if found:
                    print(
                        f"✅ Found match:\n"
                        f"Parquet:     {cnpj}, {order}, {dv}, "
                        f"{int(branch)}, {trade_name or ''}, {closing}, {opening}, {cep}\n"
                        f"CSV Match:   {int(found[0])}, {int(found[1])}, {found[2]}, "
                        f"{found[3]}, {found[4]}, {found[5]}, {found[6]}, {found[7]}"
                    )
                else:
                    self.fail(
                        f"❌ CNPJ {cnpj} from parquet not found in any estabelecimentos[0-9].csv file."
                    )


    def test_partners_subset_of_companies_and_business(self):
        print()
        print("▶️ Starting test: test_partners_subset_of_companies_and_business")
        print()

        cnpjs_partners  = set(pd.read_parquet(self.PARTNERS_PARQUET , columns=['cnpj']).cnpj)
        cnpjs_companies = set(pd.read_parquet(self.COMPANIES_PARQUET, columns=['cnpj']).cnpj)
        cnpjs_business  = set(pd.read_parquet(self.BUSINESS_PARQUET , columns=['cnpj']).cnpj)

        self.assertTrue(
            cnpjs_partners.issubset(cnpjs_companies),
            "❌ Not all partner CNPJs are present in companies.parquet"
        )
        self.assertTrue(
            cnpjs_partners.issubset(cnpjs_business),
            "❌ Not all partner CNPJs are present in business.parquet"
        )

        print(f"✅ Percentage of partner CNPJs present in companies: {(len(cnpjs_partners) / len(cnpjs_companies)) * 100:.2f}%")
        print(f"✅ Total CNPJs with data in business: {len(cnpjs_business)}")


    def test_companies_equal_business(self):
        print()
        print("▶️ Starting test: test_companies_equal_business")
        print()

        cnpjs_companies = set(pd.read_parquet(self.COMPANIES_PARQUET, columns=['cnpj']).cnpj)
        cnpjs_business  = set(pd.read_parquet(self.BUSINESS_PARQUET , columns=['cnpj']).cnpj)

        self.assertEqual(
            cnpjs_companies,
            cnpjs_business ,
            "❌ companies.parquet and business.parquet contain different CNPJs"
        )

        print("✅ CNPJs in companies and business are identical.")


if __name__ == '__main__':
    unittest.main()
