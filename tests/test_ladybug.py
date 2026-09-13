import duckdb
import unittest
import importlib

from datetime      import date
from pathlib       import Path
from unittest.mock import Mock
from contextlib    import closing
from tempfile      import TemporaryDirectory

import ladybug         as lb
import pyarrow.parquet as pq


loader     = importlib.import_module('scripts.6_load_ladybug')
load_graph = loader   .load_graph


class TestLadybug(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory(prefix='ladybug-test-')

        self.addCleanup(self.temporary.cleanup)

        self.directory = Path(self.temporary.name)
        self.source    = self.directory / "source d'água.duckdb"
        self.target    = self.directory / "graph d'agua"         / 'rfb.lbug'

        self.create_source(self.source)

    def create_source(self, source, extra_partner=None):
        with duckdb.connect(str(source)) as conn:
            conn.execute(
                '''
                CREATE TABLE companies (
                    cnpj VARCHAR, corporate_name VARCHAR, capital BIGINT
                );
                CREATE TABLE partners (
                    cnpj VARCHAR, name_partner VARCHAR, start_date VARCHAR
                );
                CREATE TABLE business (cnpj VARCHAR, trade_name VARCHAR);
                '''
            )

            conn.executemany(
                'INSERT INTO companies VALUES (?, ?, ?)',
                [
                    ('00000001', 'Água D\'Ávila "Brasil"', 123456),
                    ('00000002', 'Segunda empresa'       , 0     ),
                    ('00000003', 'Empresa isolada'       , None  ),
                    ('00000004', None                    , 700   ),
                ]
            )
            conn.executemany(
                'INSERT INTO partners VALUES (?, ?, ?)',
                [
                    ('00000001', "José D'Ávila", date(2020, 3, 1)),
                    ('00000001', "José D'Ávila", date(2019, 1, 2)),
                    ('00000001', "José D'Ávila", date(2019, 1, 2)),
                    ('00000001', "José D'Ávila", None            ),
                    ('00000002', "José D'Ávila", None            ),

                    ('00000004', '00000001'    , date(2022, 5, 6)),
            ])

            conn.execute("INSERT INTO business VALUES ('00000001', 'Filial')")

            if extra_partner is not None:
                conn.execute('INSERT INTO partners VALUES (?, ?, ?)', extra_partner)

    def build(self, source=None):
        return load_graph(
            source or self.source,
                      self.target,
            memory_limit    ='128MB'     ,
            buffer_pool_size=64 * 1024**2,
            threads         =2           ,
            batch_size      =2           ,
        )

    def rows(self, conn, query):
        with closing(conn.execute(query)) as result:
            return list(result)

    def assert_not_published(self):
        self.assertFalse(self.target.exists()                  )
        self.assertEqual(list(self.target.parent.iterdir()), [])

    def test_persisted_graph_preserves_bipartite_relationships_and_properties(self):
        original_source = self.source.read_bytes()
        counts          = self       .build     ()

        self.assertEqual(
            counts,
            {
                'Company'    : 4,
                'Partner'    : 2,
                'PARTNER_OF' : 3,
            }
        )

        self.assertEqual(self.source.read_bytes()          , original_source)
        self.assertTrue (self.target.is_file   ()          )
        self.assertEqual(list(self.target.parent.iterdir()), [self.target]  )

        with closing(
            lb.Database(
                self.target,
                read_only       =True        ,
                buffer_pool_size=64 * 1024**2,
            )
        ) as db:
            with closing(lb.Connection(db)) as conn:
                self.assertCountEqual(
                    self.rows(
                        conn,
                        '''
                        CALL show_tables() RETURN name, type
                        '''
                    ),
                    [
                        ['Company'   , 'NODE'],
                        ['PARTNER_OF', 'REL' ],
                        ['Partner'   , 'NODE'],
                    ]
                )

                self.assertEqual(
                    self.rows(
                        conn,
                        '''
                        MATCH (c:Company)
                        RETURN c.cnpj, c.corporate_name, c.capital ORDER BY c.cnpj
                        '''
                    ),
                    [
                        ['00000001', 'Água D\'Ávila "Brasil"', 123456],
                        ['00000002', 'Segunda empresa'       , 0     ],
                        ['00000003', 'Empresa isolada'       , None  ],
                        ['00000004', None                    , 700   ],
                    ]
                )

                self.assertEqual(
                    self.rows(
                        conn,
                        '''
                        MATCH (p:Partner)-[r:PARTNER_OF]->(c:Company)
                        RETURN p.name_partner, c.cnpj, r.start_date ORDER BY c.cnpj
                        '''
                    ),
                    [
                        ["José D'Ávila", '00000001', date(2019, 1, 2)],
                        ["José D'Ávila", '00000002', None            ],
                        ['00000001'    , '00000004', date(2022, 5, 6)],
                    ]
                )

                self.assertEqual(
                    self.rows(
                        conn,
                        '''
                        MATCH (a:Company)<-[:PARTNER_OF]-(p:Partner)
                              -[:PARTNER_OF]->(b:Company)
                        WHERE a.cnpj < b.cnpj
                        RETURN a.cnpj, b.cnpj
                        '''
                    ),
                    [['00000001', '00000002']]
                )

                self.assertEqual(
                    self.rows(
                        conn,
                        '''
                        MATCH (c:Company)<-[:PARTNER_OF]-(:Partner)
                        WHERE c.cnpj = '00000003' RETURN count(*)
                        '''
                    ),
                    [[0]]
                )

    def test_empty_partner_names_fail_without_publishing_or_changing_source(self):
        for index, name in enumerate((None, '', '   ')):
            with self.subTest(name=name):
                source = self.directory / f'invalid-name-{index}.duckdb'

                self.create_source(source, ('00000001', name, None))

                original_source = source.read_bytes()

                with self.assertRaisesRegex(
                    ValueError                    ,
                    'empty company or partner key',
                ):
                    self.build(source)

                self.assert_not_published()
                self.assertEqual         (
                    source.read_bytes(), original_source
                )

    def test_orphan_partner_fails_without_publishing_or_changing_source(self):
        with duckdb.connect(str(self.source)) as conn:
            conn.execute(
                "INSERT INTO partners VALUES ('99999999', 'Órfão', NULL)"
            )

        original_source = self.source.read_bytes()

        with self.assertRaisesRegex(ValueError, 'CNPJ missing from companies'):
            self.build()

        self.assert_not_published()
        self.assertEqual         (
            self.source.read_bytes(), original_source
        )

    def test_failed_native_import_cleans_up_partial_graph(self):
        with duckdb.connect(str(self.source)) as conn:
            conn.execute("INSERT INTO companies VALUES ('00000001', 'Conflito', 5)")

        original_source = self.source.read_bytes()

        with self.assertRaisesRegex(RuntimeError, '(?i)duplicate.*primary key'):
            self.build()

        self.assert_not_published()
        self.assertEqual         (
            self.source.read_bytes(), original_source
        )

    def test_malformed_date_fails_without_publishing_or_changing_source(self):
        with duckdb.connect(str(self.source)) as conn:
            conn.execute("INSERT INTO partners VALUES ('00000001', 'Data inválida', 'oops')")

        original_source = self.source.read_bytes()

        with self.assertRaises(duckdb.ConversionException):
            self.build()

        self.assert_not_published()
        self.assertEqual         (
            self.source.read_bytes(), original_source
        )

    def test_existing_output_is_never_overwritten(self):
        self.target.parent.mkdir()

        previous_output = b'existing database must stay unchanged'

        self.target.write_bytes(previous_output)

        original_source = self.source.read_bytes()

        with self.assertRaises(FileExistsError):
            self.build()

        self.assertEqual(self.target.read_bytes()          , previous_output)
        self.assertEqual(self.source.read_bytes()          , original_source)
        self.assertEqual(list(self.target.parent.iterdir()), [self.target]  )

    def test_empty_source_creates_an_empty_graph_with_the_full_schema(self):
        with duckdb.connect(str(self.source)) as conn:
            conn.execute('DELETE FROM partners; DELETE FROM companies;')

        original_source = self.source.read_bytes()

        self.assertEqual(self.build(),
            {
                'Company'    : 0,
                'Partner'    : 0,
                'PARTNER_OF' : 0,
            }
        )
        self.assertEqual(self.source.read_bytes(), original_source)

        with closing(
            lb.Database(
                self.target,
                read_only       =True        ,
                buffer_pool_size=64 * 1024**2,
            )
        ) as db:
            with closing(lb.Connection(db)) as conn:
                self.assertEqual(
                    self.rows(
                        conn, 'MATCH (n) RETURN count(*)'
                    ),
                    [[0]]
                )

                self.assertCountEqual(
                    self.rows(
                        conn,
                        '''
                        CALL show_tables() RETURN name
                        '''
                    ),
                    [
                        ['Company'   ],
                        ['PARTNER_OF'], 
                        ['Partner'   ],
                    ]
                )

    def test_batched_import_succeeds_under_memory_pressure(self):
        path = self.directory / 'large_companies.parquet'

        with duckdb.connect() as conn:
            conn.execute(
                '''
                COPY (
                    SELECT lpad(i::VARCHAR, 8, '0') AS cnpj,
                           repeat('company ', 32) || i::VARCHAR AS corporate_name,
                           i AS capital
                    FROM range(300000) t(i)
                ) TO ? (FORMAT PARQUET)
                '''
                ,
                [str(path)]
            )

        target = self.directory / 'memory.lbug'

        with closing(
            lb.Database(
                target,
                buffer_pool_size=128 * 1024**2,
                max_num_threads =2            ,
            )
        ) as db:
            with closing(lb.Connection(db)) as conn:
                conn.execute(
                    '''
                    CREATE NODE TABLE Company(
                    cnpj STRING PRIMARY KEY, corporate_name STRING, capital INT64)
                    '''
                ).close()

                with pq.ParquetFile(path) as parquet:
                    count = parquet.metadata.num_rows

                loader.import_table(conn, 'Company', path, count, batch_size=10000)

        with closing(
            lb.Database(
                target,
                read_only       =True         ,
                buffer_pool_size=128 * 1024**2,
            )
        ) as db:
            with closing(lb.Connection(db)) as conn:
                self.assertEqual(
                    self.rows(
                        conn,
                        '''
                        MATCH (c:Company) RETURN count(*), sum(c.capital)
                        '''
                    ),
                    [[300000, 300000 * 299999 // 2]]
                )

    def test_checkpoint_memory_failure_does_not_retry_committed_batch(self):
        path = self.directory / 'companies.parquet'

        with duckdb.connect(str(self.source), read_only=True) as source:
            source.execute(
                'COPY companies TO ? (FORMAT PARQUET)', [str(path)]
            )

        conn = Mock()

        conn.execute.side_effect = [
            Mock        (),
            RuntimeError(
                'Transaction committed successfully, but the post-commit '
                'checkpoint failed: The buffer pool is full'
            ),
        ]

        with self.assertRaisesRegex(RuntimeError, '--memory-limit only controls DuckDB'):
            loader.import_table(conn, 'Company', path, 4, batch_size=2)

        self.assertEqual(conn.execute.call_count    , 2              )
        self.assertEqual(conn.execute.call_args.args, ('CHECKPOINT',))


if __name__ == '__main__':
    unittest.main()
