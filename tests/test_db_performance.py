import unittest
import time
import sqlite3
import duckdb
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DUCKDB_PATH = BASE_DIR / 'data' / 'duckdb' / 'rfb.duckdb'
SQLITE_PATH = BASE_DIR / 'data' / 'sqlite' / 'rfb.sqlite3'

class TestDBPerformance(unittest.TestCase):
    """Testes de comparação de desempenho entre DuckDB e SQLite."""

    def setUp(self):
        """Configuração inicial para os testes."""
        self.duckdb_conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        self.sqlite_conn = sqlite3.connect(str(SQLITE_PATH))
        # Configurar SQLite para retornar resultados como dicionários
        self.sqlite_conn.row_factory = sqlite3.Row

    def tearDown(self):
        """Limpeza após os testes."""
        self.duckdb_conn.close()
        self.sqlite_conn.close()

    def _run_query_with_timing(self, db_type, query_func):
        """Executa uma consulta e mede o tempo de execução."""
        start_time = time.time()
        result = query_func()
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{db_type} - Tempo de execução: {execution_time:.6f} segundos")
        return result, execution_time

    def test_companies_statistics(self):
        """Testa e compara o desempenho das estatísticas de empresas."""
        print("\n=== Teste de Estatísticas de Empresas ===")
        
        # Consulta DuckDB
        def duckdb_query():
            return self.duckdb_conn.execute("""
                SELECT 
                    COUNT(*)              AS total_companies,
                    MIN(capital)          AS min_capital,
                    MAX(capital)          AS max_capital,
                    AVG(capital)          AS avg_capital
                FROM companies
            """).fetchdf().to_dict(orient="records")[0]
        
        # Consulta SQLite
        def sqlite_query():
            cursor = self.sqlite_conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*)              AS total_companies,
                    MIN(capital)          AS min_capital,
                    MAX(capital)          AS max_capital,
                    AVG(capital)          AS avg_capital
                FROM companies
            """)
            result = dict(cursor.fetchone())
            cursor.close()
            return result
        
        # Executar consultas e medir tempo
        duckdb_result, duckdb_time = self._run_query_with_timing("DuckDB", duckdb_query)
        sqlite_result, sqlite_time = self._run_query_with_timing("SQLite", sqlite_query)
        
        # Verificar se os resultados são equivalentes
        self.assertEqual(duckdb_result['total_companies'], sqlite_result['total_companies'])
        
        # Calcular e exibir a diferença de desempenho
        speedup = sqlite_time / duckdb_time if duckdb_time > 0 else float('inf')
        print(f"DuckDB é {speedup:.2f}x mais rápido que SQLite para estatísticas de empresas")

    def test_partners_statistics(self):
        """Testa e compara o desempenho das estatísticas de sócios."""
        print("\n=== Teste de Estatísticas de Sócios ===")
        
        # Consulta DuckDB
        def duckdb_query():
            return self.duckdb_conn.execute("""
                SELECT 
                    COUNT(*)              AS total_partners,
                    MIN(start_date)       AS earliest_partner_date,
                    MAX(start_date)       AS latest_partner_date
                FROM partners
            """).fetchdf().to_dict(orient="records")[0]
        
        # Consulta SQLite
        def sqlite_query():
            cursor = self.sqlite_conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*)              AS total_partners,
                    MIN(start_date)       AS earliest_partner_date,
                    MAX(start_date)       AS latest_partner_date
                FROM partners
            """)
            result = dict(cursor.fetchone())
            cursor.close()
            return result
        
        # Executar consultas e medir tempo
        duckdb_result, duckdb_time = self._run_query_with_timing("DuckDB", duckdb_query)
        sqlite_result, sqlite_time = self._run_query_with_timing("SQLite", sqlite_query)
        
        # Verificar se os resultados são equivalentes
        self.assertEqual(duckdb_result['total_partners'], sqlite_result['total_partners'])
        
        # Calcular e exibir a diferença de desempenho
        speedup = sqlite_time / duckdb_time if duckdb_time > 0 else float('inf')
        print(f"DuckDB é {speedup:.2f}x mais rápido que SQLite para estatísticas de sócios")

    def test_business_statistics(self):
        """Testa e compara o desempenho das estatísticas de negócios."""
        print("\n=== Teste de Estatísticas de Negócios ===")
        
        # Consulta DuckDB
        def duckdb_query():
            return self.duckdb_conn.execute("""
                SELECT 
                    COUNT(*)              AS total_business,
                    COUNT(DISTINCT cnpj)  AS unique_companies,
                    MIN(opening_date)     AS earliest_opening,
                    MAX(opening_date)     AS latest_opening,
                    COUNT(CASE WHEN closing_date IS NOT NULL THEN 1 END) AS closed_businesses
                FROM business
            """).fetchdf().to_dict(orient="records")[0]
        
        # Consulta SQLite
        def sqlite_query():
            cursor = self.sqlite_conn.cursor()
            cursor.execute("""
                SELECT 
                    COUNT(*)              AS total_business,
                    COUNT(DISTINCT cnpj)  AS unique_companies,
                    MIN(opening_date)     AS earliest_opening,
                    MAX(opening_date)     AS latest_opening,
                    COUNT(CASE WHEN closing_date IS NOT NULL THEN 1 END) AS closed_businesses
                FROM business
            """)
            result = dict(cursor.fetchone())
            cursor.close()
            return result
        
        # Executar consultas e medir tempo
        duckdb_result, duckdb_time = self._run_query_with_timing("DuckDB", duckdb_query)
        sqlite_result, sqlite_time = self._run_query_with_timing("SQLite", sqlite_query)
        
        # Verificar se os resultados são equivalentes
        self.assertEqual(duckdb_result['total_business'], sqlite_result['total_business'])
        
        # Calcular e exibir a diferença de desempenho
        speedup = sqlite_time / duckdb_time if duckdb_time > 0 else float('inf')
        print(f"DuckDB é {speedup:.2f}x mais rápido que SQLite para estatísticas de negócios")

    def test_all_statistics_combined(self):
        """Testa e compara o desempenho de todas as estatísticas combinadas."""
        print("\n=== Teste de Todas as Estatísticas Combinadas ===")
        
        # Função para obter todas as estatísticas do DuckDB
        def duckdb_all_stats():
            stats = {}
            
            # Companies
            stats['companies'] = self.duckdb_conn.execute("""
                SELECT 
                    COUNT(*)              AS total_companies,
                    MIN(capital)          AS min_capital,
                    MAX(capital)          AS max_capital,
                    AVG(capital)          AS avg_capital
                FROM companies
            """).fetchdf().to_dict(orient="records")[0]
            
            # Partners
            stats['partners'] = self.duckdb_conn.execute("""
                SELECT 
                    COUNT(*)              AS total_partners,
                    MIN(start_date)       AS earliest_partner_date,
                    MAX(start_date)       AS latest_partner_date
                FROM partners
            """).fetchdf().to_dict(orient="records")[0]
            
            # Business
            stats['business'] = self.duckdb_conn.execute("""
                SELECT 
                    COUNT(*)              AS total_business,
                    COUNT(DISTINCT cnpj)  AS unique_companies,
                    MIN(opening_date)     AS earliest_opening,
                    MAX(opening_date)     AS latest_opening,
                    COUNT(CASE WHEN closing_date IS NOT NULL THEN 1 END) AS closed_businesses
                FROM business
            """).fetchdf().to_dict(orient="records")[0]
            
            return stats
        
        # Função para obter todas as estatísticas do SQLite
        def sqlite_all_stats():
            stats = {}
            cursor = self.sqlite_conn.cursor()
            
            # Companies
            cursor.execute("""
                SELECT 
                    COUNT(*)              AS total_companies,
                    MIN(capital)          AS min_capital,
                    MAX(capital)          AS max_capital,
                    AVG(capital)          AS avg_capital
                FROM companies
            """)
            stats['companies'] = dict(cursor.fetchone())
            
            # Partners
            cursor.execute("""
                SELECT 
                    COUNT(*)              AS total_partners,
                    MIN(start_date)       AS earliest_partner_date,
                    MAX(start_date)       AS latest_partner_date
                FROM partners
            """)
            stats['partners'] = dict(cursor.fetchone())
            
            # Business
            cursor.execute("""
                SELECT 
                    COUNT(*)              AS total_business,
                    COUNT(DISTINCT cnpj)  AS unique_companies,
                    MIN(opening_date)     AS earliest_opening,
                    MAX(opening_date)     AS latest_opening,
                    COUNT(CASE WHEN closing_date IS NOT NULL THEN 1 END) AS closed_businesses
                FROM business
            """)
            stats['business'] = dict(cursor.fetchone())
            
            cursor.close()
            return stats
        
        # Executar consultas e medir tempo
        duckdb_result, duckdb_time = self._run_query_with_timing("DuckDB", duckdb_all_stats)
        sqlite_result, sqlite_time = self._run_query_with_timing("SQLite", sqlite_all_stats)
        
        # Verificar se os resultados são equivalentes
        self.assertEqual(duckdb_result['companies']['total_companies'], 
                         sqlite_result['companies']['total_companies'])
        
        # Calcular e exibir a diferença de desempenho
        speedup = sqlite_time / duckdb_time if duckdb_time > 0 else float('inf')
        print(f"DuckDB é {speedup:.2f}x mais rápido que SQLite para todas as estatísticas combinadas")


if __name__ == '__main__':
    unittest.main()