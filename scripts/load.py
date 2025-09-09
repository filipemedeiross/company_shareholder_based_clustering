import time
import pyarrow.parquet as pq


def insert_parquet(
    conn             ,
    table_name       ,
    parquet_file     ,
    duplicates=None  ,
    batch_size=32_768,
):
    print()
    print(f"📥 Loading and inserting data into '{table_name}' from Parquet (by row group)...")

    if duplicates:
        seen = set()

    table = pq.ParquetFile(parquet_file)
    for c, batch in enumerate(table.iter_batches(batch_size=batch_size)):
        if (c % 50) == 0:
            print(f"  • Processed {c} batches...")

        df = batch.to_pandas()

        for col in df.select_dtypes(include=["datetime64[ns]"]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")

        if duplicates:
            df = df.drop_duplicates(
                subset=duplicates,
                keep='first'     ,
            )
            df = df[~df[duplicates].isin(seen)]

            seen.update(df[duplicates])

        df.to_sql(
            table_name        ,
            conn              ,
            index=False       ,
            if_exists='append',
        )

        del df


def measure_query_time(cursor, query, label):
    start = time.perf_counter()
    cursor.execute(query).fetchall()
    end   = time.perf_counter()

    print(f"⏱️ Query time for {label}: {end - start:.2f} seconds")
