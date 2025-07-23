import csv
import random
import unittest
import pandas as pd

from pathlib     import Path
from fastparquet import ParquetFile


class TestParquet(unittest.TestCase):
    ROOT_DIR = Path(__file__).resolve().parent.parent

    PARTNERS_PARQUET = ROOT_DIR / 'data/parquet/partners.parquet'
    BUSINESS_PARQUET = ROOT_DIR / 'data/parquet/business.parquet'


    def get_sample_from_parquet(self, filename, n=10):
        samples = []

        with open(filename, mode='rb') as f:
            pf = ParquetFile(f)

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

        return pd.concat(samples)


    def find_row(self, files, cnpj, partner, start):
        for csv_path in files:
            with open(csv_path, encoding="latin-1", newline='') as f:
                reader = csv.reader(f, delimiter=';', quotechar='"')

                for row in reader:
                    if not row:
                        continue

                    try:
                        cnpj_csv, partner_csv, start_csv = row[0], row[2], row[5]
                    except IndexError:
                        continue

                    if (
                        cnpj    == int(cnpj_csv)  and
                        partner == partner_csv    and
                        start   == int(start_csv)
                    ):
                        return cnpj_csv, partner_csv, start_csv

        return None


    def test_partners_parquet_contains_csv_data(self):
        SOCIOS_PATH = [
            self.ROOT_DIR / f'data/csv/socios/socios{i}.csv'
            for i in range(10)
        ]

        sample = self.get_sample_from_parquet(self.PARTNERS_PARQUET)

        for _, (cnpj, partner, start) in sample.iterrows():
            row_found = self.find_row(SOCIOS_PATH, cnpj, partner, start)

            with self.subTest(cnpj=cnpj, partner=partner, start=start):
                if row_found:
                    print(
                        f"✅ Found match:\nParquet:     {cnpj}, {partner}, {start}\nCSV Match:   {row_found[0]}, {row_found[1]}, {row_found[2]}"
                    )
                else:
                    self.fail(
                        f"❌ CNPJ {cnpj} from parquet not found in any socios[0-9].csv file."
                    )

        del sample


if __name__ == '__main__':
    unittest.main()
