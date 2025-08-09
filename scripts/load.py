import time
import pyarrow.parquet as pq


def insert_parquet(
    conn           ,
    table_name     ,
    parquet_file   ,
    duplicates=None,
):
    print()
    print(f"📥 Loading and inserting data into '{table_name}' from Parquet (by row group)...")

    if duplicates:
        seen = set()

    table = pq.ParquetFile(parquet_file)
    for i in range(table.num_row_groups):
        print(f"  • Processing row_group {i + 1}...")

        df = table.read_row_group(i).to_pandas()

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
