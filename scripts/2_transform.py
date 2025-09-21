import duckdb
import pandas as pd

from .process   import csv2parquet
from .constants import CHUNKSIZE,         \
                       COLS_PARTNERS,     \
                       COLS_COMPANIES,    \
                       COLS_BUSINESS,     \
                       NAMES_PARTNERS,    \
                       NAMES_COMPANIES,   \
                       NAMES_BUSINESS,    \
                       SORT_PARTNERS,     \
                       SORT_COMPANIES,    \
                       SORT_BUSINESS,     \
                       DICT_DIR,          \
                       PARQUET_DIR,       \
                       PARQUET_PARTNERS,  \
                       PARQUET_COMPANIES, \
                       PARQUET_BUSINESS


con = duckdb.connect()


def fn_partners(df):
    df.dropna(inplace=True)

    df.cnpj = df.cnpj.astype(str).str.zfill(8)
    df.start_date = pd.to_datetime(
        df
        .start_date
        .astype(str)   ,
        format="%Y%m%d",
        errors="coerce",
    )

    return df


def fn_companies(df):
    df.cnpj = df.cnpj.astype(str).str.zfill(8)
    df.capital = (
        df
        .capital
        .str
        .replace(r',.*', '', regex=True)
        .astype('int64')
    )

    query = f"""
        SELECT DISTINCT ON (cnpj) df.*
        FROM df
        SEMI JOIN read_parquet('{PARQUET_PARTNERS}') USING (cnpj)
    """

    return con.execute(query).df()


def fn_business(df):
    df.branch = df.branch.astype(str).str.strip() == '1'

    df.cep = (
        df
        .cep
        .fillna('0')
        .astype(str)
        .str.replace('-' , '')
        .str.replace(r'\..*$', '', regex=True)
        .str.zfill(8)
    )

    df.cnpj       = df.cnpj      .astype(str).str.zfill(8)
    df.cnpj_order = df.cnpj_order.astype(str).str.zfill(4)
    df.cnpj_dv    = df.cnpj_dv   .astype(str).str.zfill(2)

    df.closing_date = pd.to_datetime(
        df
        .closing_date
        .astype(str)   ,
        format="%Y%m%d",
        errors="coerce",
    )
    df.opening_date = pd.to_datetime(
        df
        .opening_date
        .astype(str)   ,
        format="%Y%m%d",
        errors="coerce",
    )

    query = f"""
        SELECT df.*
        FROM df
        SEMI JOIN read_parquet('{PARQUET_PARTNERS}') USING (cnpj)
    """

    return con.execute(query).df()


def main():
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    if PARQUET_PARTNERS.exists():
        print('Skipping partners.parquet — already exists.')
    else:
        paths_partners = [
            DICT_DIR['Socios'] / f'socios{i}.csv'
            for i in range(10)
        ]

        csv2parquet(
            PARQUET_PARTNERS     ,
            paths_partners       ,
            COLS_PARTNERS        ,
            NAMES_PARTNERS       ,
            CHUNKSIZE            ,
            transform=fn_partners,
            sort_by=SORT_PARTNERS,
        )

    if PARQUET_COMPANIES.exists():
        print('Skipping companies.parquet — already exists.')
    else:
        paths_companies = [
            DICT_DIR['Empresas'] / f'empresas{i}.csv'
            for i in range(10)
        ]

        csv2parquet(
            PARQUET_COMPANIES     ,
            paths_companies       ,
            COLS_COMPANIES        ,
            NAMES_COMPANIES       ,
            CHUNKSIZE             ,
            transform=fn_companies,
            sort_by=SORT_COMPANIES,
        )

    if PARQUET_BUSINESS.exists():
        print('Skipping business.parquet — already exists.')
    else:
        paths_business = [
            DICT_DIR['Estabelecimentos'] / f'estabelecimentos{i}.csv'
            for i in range(10)
        ]

        csv2parquet(
            PARQUET_BUSINESS     ,
            paths_business       ,
            COLS_BUSINESS        ,
            NAMES_BUSINESS       ,
            CHUNKSIZE            ,
            transform=fn_business,
            sort_by=SORT_BUSINESS,
        )


if __name__ == '__main__':
    main()
