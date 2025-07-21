import pandas as pd
from pathlib import Path


CHUNKSIZE = 3000000

COLS_PARTNERS = [0, 2, 5]
COLS_BUSINESS = [0, 4, 6, 10, 18]

NAMES_PARTNERS = [
    'cnpj'             ,
    'name_partner'     ,
    'partnership_start',
]
NAMES_BUSINESS = [
    'cnpj'        ,
    'trade_name'  ,
    'closing_date',
    'opening_date',
    'cep'         ,
]

ROOT_DIR    = Path(__file__).resolve().parent.parent
OUTPUT_PARTNERS = ROOT_DIR / 'data/parquet/partners.parquet'
OUTPUT_BUSINESS = ROOT_DIR / 'data/parquet/business.parquet'


OUTPUT_PARTNERS.parent.mkdir(parents=True, exist_ok=True)
OUTPUT_BUSINESS.parent.mkdir(parents=True, exist_ok=True)


# ==========================================================================
# PART 1 - Generate initial partners.parquet incrementally using fastparquet
# ==========================================================================
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
    df.cnpj              = df.cnpj.astype('int32')
    df.partnership_start = pd.to_datetime(
        df.partnership_start,
        format='%Y%m%d'     ,
        errors='coerce'     ,
    )

    df.to_parquet(
        OUTPUT_PARTNERS                ,
        engine='fastparquet'           ,
        index=False                    ,
        append=not first_write_partners,
    )

    first_write_partners = False
    del df


# ==========================================================================
# PART 2 - Generate initial business.parquet incrementally using fastparquet
# ==========================================================================
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
        chunk.cep = (
            chunk.cep
            .fillna('0')
            .astype(str)
            .str.replace('-', '', regex=True)
            .astype(float)
            .astype(int)
        )

        chunk.to_parquet(
            OUTPUT_BUSINESS                ,
            engine='fastparquet'           ,
            index=False                    ,
            append=not first_write_business,
        )

        first_write_business = False
        del chunk


# ==========================================================================
# PART 3 - Reload both files and rewrite with higher compression (Zstandard)
# ==========================================================================
partners = pd.read_parquet(OUTPUT_PARTNERS)
partners.to_parquet(
    OUTPUT_PARTNERS   ,
    engine='pyarrow'  ,
    index=False       ,
)

business = pd.read_parquet(OUTPUT_BUSINESS)
business.to_parquet(
    OUTPUT_BUSINESS   ,
    engine='pyarrow'  ,
    index=False       ,
)
