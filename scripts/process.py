import pandas as pd


def csv2parquet(
    parquet_path  ,
    file_paths    ,
    usecols       ,
    names         ,
    chunksize     ,
    transform=None,
    sort_by=None  ,
):
    first_write = True

    for file_path in file_paths:
        print(f"[Transform] Reading {file_path.name}")

        chunks = pd.read_csv(
            file_path          ,
            sep=';'            ,
            usecols=usecols    ,
            names=names        ,
            chunksize=chunksize,
            low_memory=False   ,
            encoding='latin-1' ,
            on_bad_lines='skip',
        )

        for chunk in chunks:
            if transform:
                chunk = transform(chunk)

            if sort_by:
                chunk.sort_values(sort_by, inplace=True)

            chunk.to_parquet(
                parquet_path          ,
                engine='fastparquet'  ,
                index=False           ,
                append=not first_write,
            )

            first_write = False
