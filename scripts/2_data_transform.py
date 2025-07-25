import pandas as pd

from pathlib import Path


CHUNKSIZE = 3000000

COLS_PARTNERS  = [
    0, 2, 5
]
COLS_COMPANIES = [
    0, 1, 4
]
COLS_BUSINESS  = [
    0, 1, 2, 3, 4, 6, 10, 18
]

NAMES_PARTNERS  = [
    'cnpj'        ,
    'name_partner',
    'start_date'  ,
]
NAMES_COMPANIES = [
    'cnpj'          ,
    'corporate_name',
    'capital'       ,
]
NAMES_BUSINESS  = [
    'cnpj'        ,
    'cnpj_order'  ,
    'cnpj_dv'     ,
    'branch'      ,
    'trade_name'  ,
    'closing_date',
    'opening_date',
    'cep'         ,
]

ROOT_DIR    = Path(__file__).resolve().parent.parent
PARQUET_DIR = ROOT_DIR / 'data/parquet/'

OUTPUT_PARTNERS  = PARQUET_DIR / 'partners.parquet'
OUTPUT_COMPANIES = PARQUET_DIR / 'companies.parquet'
OUTPUT_BUSINESS  = PARQUET_DIR / 'business.parquet'


PARQUET_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================================================
# PART 1 - Generate initial partners.parquet incrementally using fastparquet
# ==========================================================================
if not OUTPUT_PARTNERS.exists():
    first_write_partners = True

    for i in range(10):
        file_path = ROOT_DIR / f'data/csv/socios/socios{i}.csv'

        print('Reading', file_path)

        df = pd.read_csv(
            file_path            ,
            sep=';'              ,
            usecols=COLS_PARTNERS,
            names=NAMES_PARTNERS ,
            encoding='latin-1'   ,
            on_bad_lines='skip'  ,
        )
        df.dropna()

        df.cnpj       = df.cnpj.astype('int32')
        df.start_date = df.start_date.astype('int32')

        df.to_parquet(
            OUTPUT_PARTNERS                ,
            engine='fastparquet'           ,
            index=False                    ,
            append=not first_write_partners,
        )

        first_write_partners = False

        del df
else:
    print('Skipping partners.parquet — already exists.')


# ===========================================================================
# PART 2 - Generate initial companies.parquet incrementally using fastparquet
# ===========================================================================
if not OUTPUT_COMPANIES.exists():
    first_write_companies = True

    for i in range(10):
        file_path = ROOT_DIR / f'data/csv/empresas/empresas{i}.csv'
        print('Reading', file_path)

        for chunk in pd.read_csv(
            file_path             ,
            sep=';'               ,
            usecols=COLS_COMPANIES,
            names=NAMES_COMPANIES ,
            chunksize=CHUNKSIZE   ,
            low_memory=False      ,
            encoding='latin-1'    ,
            on_bad_lines='skip'   ,
        ):
            chunk.cnpj    = chunk.cnpj.astype('int32')
            chunk.capital = (
                chunk.capital
                .str.replace('.',  '', regex=False)
                .str.replace(',', '.', regex=False)
                .astype(float)
            )

            chunk.to_parquet(
                OUTPUT_COMPANIES                ,
                engine='fastparquet'            ,
                index=False                     ,
                append=not first_write_companies,
            )

            first_write_companies = False

            del chunk
else:
    print('Skipping companies.parquet — already exists.')


# ==========================================================================
# PART 3 - Generate initial business.parquet incrementally using fastparquet
# ==========================================================================
if not OUTPUT_BUSINESS.exists():
    first_write_business = True

    for i in range(10):
        file_path = ROOT_DIR / f'data/csv/estabelecimentos/estabelecimentos{i}.csv'
        print('Reading', file_path)

        for chunk in pd.read_csv(
            file_path            ,
            sep=';'              ,
            usecols=COLS_BUSINESS,
            names=NAMES_BUSINESS ,
            chunksize=CHUNKSIZE  ,
            low_memory=False     ,
            encoding='latin-1'   ,
            on_bad_lines='skip'  ,
        ):
            chunk.branch = (
                chunk.branch
                .astype(str)
                .str
                .strip()
                == '1'
            )
            chunk.cep = (
                chunk.cep
                .fillna('0')
                .astype(str)
                .str.replace('-', '', regex=True)
                .astype(float)
                .astype('int32')
            )

            chunk.cnpj         = chunk.cnpj.astype        ('int32')
            chunk.cnpj_order   = chunk.cnpj_order.astype  ('int16')
            chunk.cnpj_dv      = chunk.cnpj_dv.astype     ('int8' )
            chunk.closing_date = chunk.closing_date.astype('int32')
            chunk.opening_date = chunk.opening_date.astype('int32')

            chunk.to_parquet(
                OUTPUT_BUSINESS                ,
                engine='fastparquet'           ,
                index=False                    ,
                append=not first_write_business,
            )

            first_write_business = False

            del chunk
else:
    print('Skipping business.parquet — already exists.')


# ==============================================================
# PART 4 - Reload both files and rewrite with higher compression
# ==============================================================
print('Rewriting parquet files with pyarrow compression...')

if not OUTPUT_PARTNERS.exists():
    partners = pd.read_parquet(OUTPUT_PARTNERS)

    partners.sort_values(
        by=[
            'start_date'  ,
            'name_partner',
        ],
        inplace=True,
    )
    partners.to_parquet(
        OUTPUT_PARTNERS ,
        engine='pyarrow',
        index=False     ,
    )

    del partners


if not OUTPUT_BUSINESS.exists():
    business = pd.read_parquet(OUTPUT_BUSINESS)

    business.sort_values(
        by=[
            'closing_date',
            'opening_date',
            'cep'         ,
        ],
        inplace=True,
    )
    business.to_parquet(
        OUTPUT_BUSINESS ,
        engine='pyarrow',
        index=False     ,
    )

    del business
