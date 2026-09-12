import os
import duckdb
import argparse

from pathlib    import Path
from contextlib import ExitStack, closing
from tempfile   import TemporaryDirectory

import ladybug         as lb
import pyarrow         as pa
import pyarrow.parquet as pq

from .constants import DUCKDB_PATH, LADYBUG_PATH


def export_tables(source, directory, memory_limit, threads):
    """Export with DuckDB's disk spill support, without materializing in Python."""
    queries = {
        'Company'    : 'SELECT          cnpj, corporate_name, capital FROM companies',
        'Partner'    : 'SELECT DISTINCT name_partner                  FROM partners' ,
        'PARTNER_OF' : '''
            SELECT   name_partner, cnpj, MIN(CAST(start_date AS DATE)) AS start_date
            FROM     partners
            GROUP BY name_partner, cnpj
        ''',
    }

    exports = {}

    with duckdb.connect(
        str(source),

        read_only=True,
        config   ={
            'memory_limit'   : memory_limit                   ,
            'threads'        : threads                        ,
            'temp_directory' : str(directory / 'duckdb_spill'),
        }
    ) as conn:
        if conn.execute(
            '''
            SELECT 1 FROM partners
            WHERE cnpj         IS NULL OR trim(cnpj        ) = '' OR
                  name_partner IS NULL OR trim(name_partner) = ''
            LIMIT 1
            '''
        ).fetchone():
            raise ValueError('Partners contains an empty company or partner key.')

        if conn.execute(
            '''
            SELECT 1
            FROM      partners  p
            ANTI JOIN companies c
            ON p.cnpj = c.cnpj
            LIMIT 1
            '''
        ).fetchone():
            raise ValueError('partners contains a CNPJ missing from companies.')

        for table, query in queries.items():
            path = directory / f'{table}.parquet'

            print(f'Exporting {table} from DuckDB...', flush=True)
            conn.execute(
                f'COPY ({query}) TO ? (FORMAT PARQUET)', [str(path)]
            )

            with pq.ParquetFile(path) as parquet:
                count = parquet.metadata.num_rows

            exports[table] = (path, count)

    return exports


def import_table(conn, table, path, count, batch_size):
    """Bound each COPY transaction and flush it before loading the next batch."""
    batch_path   = path.parent / 'import_batch.parquet'
    escaped_path = batch_path.as_posix().replace("'", "\\'")

    loaded = 0

    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(
            batch_size =batch_size,
            use_threads=False     ,
        ):
            pq.write_table(pa.Table.from_batches([batch]), batch_path)

            try:
                conn.execute(
                    f"COPY {table} FROM '{escaped_path}' (IGNORE_ERRORS=false)"
                ).close()

                conn.execute('CHECKPOINT').close()
            except RuntimeError as exc:
                if 'buffer pool is full' not in str(exc).lower():
                    raise

                raise RuntimeError(
                    f'Ladybug ran out of buffer memory loading {table} after '
                    f'{loaded:,} checkpointed records '
                    f'(batch size: {batch_size:,}). '
                    'Rerun with a smaller --batch-size or, if RAM is available, '
                    'a larger --buffer-pool-mb. --memory-limit only controls DuckDB.'
                ) from exc

            loaded += batch.num_rows

            print(f'  {table}: {loaded:,}/{count:,} records saved.', flush=True)

    if loaded != count:
        raise RuntimeError(f'{table}: expected {count:,} records, read {loaded:,}.')


def load_graph(
    source=DUCKDB_PATH ,
    target=LADYBUG_PATH,
    *,
    memory_limit    ='1GB'  ,
    buffer_pool_size=1024**3,
    threads         =4      ,
    batch_size      =100_000,
):
    """Publish a complete graph; leave the source and existing targets intact."""
    source = Path(source).resolve()
    target = Path(target).resolve()

    if not source.is_file():
        raise FileNotFoundError(f'DuckDB database not found: {source}')
    if target.exists():
        raise FileExistsError(f'Ladybug database already exists: {target}')

    if threads < 1 or buffer_pool_size < 1 or batch_size < 1:
        raise ValueError('Threads, buffer_pool_size and batch_size must be positive.')

    target.parent.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(prefix='.ladybug-load-', dir=target.parent) as tmp:
        directory = Path(tmp)
        staged_db = directory / 'graph.lbug'

        with closing(
            lb.Database(
                staged_db,
                buffer_pool_size=buffer_pool_size,
                max_num_threads =threads         ,
            )
        ):
            pass

        exports = export_tables(source, directory, memory_limit, threads)

        with closing(
            lb.Database(
                staged_db,
                buffer_pool_size=buffer_pool_size,
                max_num_threads =threads         ,
            )
        ) as db:
            with closing(lb.Connection(db)) as conn:
                for statement in (
                    '''
                    CREATE NODE TABLE Company(
                        cnpj           STRING PRIMARY KEY,
                        corporate_name STRING,
                        capital        INT64
                    )
                    ''',

                    '''
                    CREATE NODE TABLE Partner(
                        name_partner STRING PRIMARY KEY
                    )
                    ''',

                    '''
                    CREATE REL TABLE PARTNER_OF(
                        FROM Partner TO Company, start_date DATE
                    )
                    ''',
                ):
                    conn.execute(statement).close()

                for table, (path, count) in exports.items():
                    print(f'Loading {table}: {count:,} records...', flush=True)

                    import_table(conn, table, path, count, batch_size)

                conn.execute('CHECKPOINT').close()

        if target.exists():
            raise FileExistsError(f'Ladybug database already exists: {target}')

        staged_db.rename(target)

    print(f'Bipartite graph saved to: {target}', flush=True)

    return {
        table : count
        for table, (_, count) in exports.items()
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('--source'        , type=Path, default=DUCKDB_PATH , help='Source DuckDB database (opened read-only).'                      )
    parser.add_argument('--output'        , type=Path, default=LADYBUG_PATH, help='New Ladybug database path; must not already exist.'              )
    parser.add_argument('--memory-limit'  ,            default='1GB'       , help='DuckDB memory limit; larger operations spill to disk.'           )
    parser.add_argument('--buffer-pool-mb', type=int , default=1024        , help='Ladybug buffer pool size in MiB.'                                )
    parser.add_argument('--threads'       , type=int , default=4                                                                                    )
    parser.add_argument('--batch-size'    , type=int , default=100_000     , help='Maximum records per Ladybug COPY, checkpointed after each batch.')
    parser.add_argument('--dll-directory' , type=Path,                       help='Windows directory containing Ladybug runtime DLLs.'              )

    args = parser.parse_args()

    with ExitStack() as stack:
        if args.dll_directory is not None:
            if os.name != 'nt':
                parser.error('--dll-directory is only supported on Windows.')

            stack.enter_context(os.add_dll_directory(
                str(args.dll_directory.resolve())
            ))

        load_graph(
            args.source,
            args.output,
            memory_limit    =args.memory_limit            ,
            buffer_pool_size=args.buffer_pool_mb * 1024**2,
            threads         =args.threads                 ,
            batch_size      =args.batch_size              ,
        )


if __name__ == '__main__':
    main()
