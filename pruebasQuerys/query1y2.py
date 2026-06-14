#!/usr/bin/env python3
"""
Script para comparar rendimiento PostgreSQL vs Spark
Optimizado para queries específicas por motor
"""

import psycopg2
from pyspark.sql import SparkSession
import time
import os
import sys
from typing import Dict, List, Optional
from datetime import datetime

class EngineComparator:
 def __init__(self, warmup: bool = False):
     """
     Inicializa ambos motores de base de datos
     
     Args:
         warmup: Si es True, ejecuta queries de calentamiento para poblar cachés
     """
     print("🔧 Inicializando motores de base de datos...")
     
     # PostgreSQL
     try:
         self.pg_conn = psycopg2.connect(
             dbname="tpch_benchmark",
             user=os.getenv("USER"),
             host="localhost"
         )
         self.pg_conn.autocommit = True
         print("   ✅ PostgreSQL conectado")
     except Exception as e:
         print(f"   ❌ Error conectando PostgreSQL: {e}")
         sys.exit(1)
     
     # Spark
     try:
         self.spark = SparkSession.builder \
             .appName("Engine Comparison") \
             .master("local[*]") \
             .config("spark.driver.memory", "4g") \
             .config("spark.sql.shuffle.partitions", "200") \
             .getOrCreate()
         
         self.spark.sparkContext.setLogLevel("ERROR")
         print("   ✅ Spark inicializado")
     except Exception as e:
         print(f"   ❌ Error inicializando Spark: {e}")
         sys.exit(1)
     
     # Cargar tablas en Spark
     print("   📦 Cargando tablas TPC-H en Spark...")
     tables = ["region", "nation", "customer", "supplier", 
               "part", "partsupp", "orders", "lineitem"]
     
     for table in tables:
         try:
             df = self.spark.read.parquet(f"tpch-dbgen/parquet_data/{table}")
             df.createOrReplaceTempView(table)
             print(f"      • {table}: {df.count()} filas")
         except Exception as e:
             print(f"      ❌ Error cargando {table}: {e}")
             sys.exit(1)
     
     print("\n✅ Ambos motores inicializados correctamente\n")
     
     # Warmup si se solicita
     if warmup:
         self._warmup()
 
 def _warmup(self):
     """Ejecuta queries simples para calentar cachés"""
     print("🔥 Calentando cachés...\n")
     warmup_query = "SELECT COUNT(*) FROM lineitem"
     
     try:
         cur = self.pg_conn.cursor()
         cur.execute(warmup_query)
         cur.fetchall()
         cur.close()
         print("   ✅ PostgreSQL cache caliente")
     except:
         pass
     
     try:
         self.spark.sql(warmup_query).collect()
         print("   ✅ Spark cache caliente\n")
     except:
         pass
 
 def run_postgres(self, query: str, name: str = "Query") -> Optional[Dict]:
     """
     Ejecuta query en PostgreSQL con manejo de errores
     
     Args:
         query: Query SQL a ejecutar
         name: Nombre descriptivo de la query
         
     Returns:
         Dict con resultados o None si falla
     """
     print(f"🐘 PostgreSQL - {name}...", end=" ", flush=True)
     
     try:
         cur = self.pg_conn.cursor()
         
         start = time.time()
         cur.execute(query)
         results = cur.fetchall()
         elapsed = time.time() - start
         
         cur.close()
         print(f"⏱️  {elapsed:.3f}s ({len(results)} filas)")
         
         return {
             "engine": "PostgreSQL",
             "time": elapsed,
             "rows": len(results),
             "status": "success",
             "results": results[:5]  # Primeras 5 filas para verificación
         }
     except Exception as e:
         print(f"❌ ERROR: {str(e)[:100]}")
         return {
             "engine": "PostgreSQL",
             "time": None,
             "rows": 0,
             "status": "error",
             "error": str(e)
         }
 
 def run_spark(self, query: str, name: str = "Query") -> Optional[Dict]:
     """
     Ejecuta query en Spark con manejo de errores
     
     Args:
         query: Query SQL a ejecutar
         name: Nombre descriptivo de la query
         
     Returns:
         Dict con resultados o None si falla
     """
     print(f"⚡ Spark - {name}...", end=" ", flush=True)
     
     try:
         start = time.time()
         results = self.spark.sql(query).collect()
         elapsed = time.time() - start
         
         print(f"⏱️  {elapsed:.3f}s ({len(results)} filas)")
         
         return {
             "engine": "Spark",
             "time": elapsed,
             "rows": len(results),
             "status": "success",
             "results": results[:5]  # Primeras 5 filas para verificación
         }
     except Exception as e:
         print(f"❌ ERROR: {str(e)[:100]}")
         return {
             "engine": "Spark",
             "time": None,
             "rows": 0,
             "status": "error",
             "error": str(e)
         }
 
 def compare_query(self, query: str, name: str = "Query") -> Dict:
     """
     Compara la misma query en ambos motores
     
     Args:
         query: Query SQL a ejecutar
         name: Nombre descriptivo
         
     Returns:
         Dict con resultados comparativos
     """
     print(f"\n{'='*70}")
     print(f"📊 {name}")
     print('='*70)
     
     pg_result = self.run_postgres(query, name)
     spark_result = self.run_spark(query, name)
     
     # Análisis de resultados
     print(f"\n📈 Análisis:")
     
     if pg_result["status"] == "error" or spark_result["status"] == "error":
         print("   ⚠️  Una o ambas queries fallaron")
         winner = None
         speedup = None
     else:
         if pg_result["time"] < spark_result["time"]:
             speedup = spark_result["time"] / pg_result["time"]
             winner = "PostgreSQL"
             print(f"   🏆 PostgreSQL fue {speedup:.2f}x más rápido")
         else:
             speedup = pg_result["time"] / spark_result["time"]
             winner = "Spark"
             print(f"   🏆 Spark fue {speedup:.2f}x más rápido")
         
         print(f"   • PostgreSQL: {pg_result['time']:.3f}s")
         print(f"   • Spark:      {spark_result['time']:.3f}s")
         print(f"   • Diferencia: {abs(pg_result['time'] - spark_result['time']):.3f}s")
         
         # Verificar consistencia de resultados
         if pg_result["rows"] != spark_result["rows"]:
             print(f"   ⚠️  ADVERTENCIA: Diferente número de filas!")
             print(f"      PostgreSQL: {pg_result['rows']} | Spark: {spark_result['rows']}")
     
     return {
         "query_name": name,
         "postgres": pg_result,
         "spark": spark_result,
         "winner": winner,
         "speedup": speedup
     }
 
 def run_postgres_only(self, query: str, name: str = "Query") -> Dict:
     """
     Ejecuta query solo en PostgreSQL (para queries optimizadas PG)
     """
     print(f"\n{'='*70}")
     print(f"📊 {name} [SOLO POSTGRESQL]")
     print('='*70)
     
     result = self.run_postgres(query, name)
     
     print(f"\n📈 Resultado:")
     if result["status"] == "success":
         print(f"   ✅ Ejecutada exitosamente en {result['time']:.3f}s")
         print(f"   • Filas retornadas: {result['rows']}")
     else:
         print(f"   ❌ Error en ejecución")
     
     return {
         "query_name": name,
         "engine": "PostgreSQL",
         "result": result
     }
 
 def run_spark_only(self, query: str, name: str = "Query") -> Dict:
     """
     Ejecuta query solo en Spark (para queries optimizadas Spark)
     """
     print(f"\n{'='*70}")
     print(f"📊 {name} [SOLO SPARK]")
     print('='*70)
     
     result = self.run_spark(query, name)
     
     print(f"\n📈 Resultado:")
     if result["status"] == "success":
         print(f"   ✅ Ejecutada exitosamente en {result['time']:.3f}s")
         print(f"   • Filas retornadas: {result['rows']}")
     else:
         print(f"   ❌ Error en ejecución")
     
     return {
         "query_name": name,
         "engine": "Spark",
         "result": result
     }
 
 def close(self):
     """Cierra conexiones"""
     self.pg_conn.close()
     self.spark.stop()
     print("\n🔌 Conexiones cerradas")

def print_summary(all_results: List[Dict]):
 """
 Imprime resumen final formateado con tabla comparativa
 """
 print("\n\n" + "="*90)
 print("📊 RESUMEN FINAL - COMPARACIÓN DE RENDIMIENTO")
 print("="*90)
 
 # Separar resultados por tipo
 comparisons = [r for r in all_results if "winner" in r]
 pg_only = [r for r in all_results if r.get("engine") == "PostgreSQL"]
 spark_only = [r for r in all_results if r.get("engine") == "Spark"]
 
 # Tabla de comparaciones directas
 if comparisons:
     print("\n🔀 COMPARACIONES DIRECTAS (misma query en ambos motores):")
     print("-" * 90)
     print(f"{'Query':<45} {'PostgreSQL':>12} {'Spark':>12} {'Ganador':>15}")
     print("-" * 90)
     
     pg_wins = 0
     spark_wins = 0
     
     for r in comparisons:
         pg_time = r["postgres"]["time"]
         spark_time = r["spark"]["time"]
         
         if r["winner"]:
             if r["winner"] == "PostgreSQL":
                 pg_wins += 1
                 winner_str = f"PG ({r['speedup']:.2f}x)"
             else:
                 spark_wins += 1
                 winner_str = f"Spark ({r['speedup']:.2f}x)"
             
             pg_str = f"{pg_time:.3f}s" if pg_time else "ERROR"
             spark_str = f"{spark_time:.3f}s" if spark_time else "ERROR"
         else:
             pg_str = "ERROR"
             spark_str = "ERROR"
             winner_str = "N/A"
         
         query_name = r["query_name"][:44]
         print(f"{query_name:<45} {pg_str:>12} {spark_str:>12} {winner_str:>15}")
     
     print("-" * 90)
     print(f"\n🏆 Score: PostgreSQL {pg_wins} - {spark_wins} Spark")
 
 # Queries solo PostgreSQL
 if pg_only:
     print("\n\n🐘 QUERIES OPTIMIZADAS PARA POSTGRESQL:")
     print("-" * 70)
     print(f"{'Query':<50} {'Tiempo':>15}")
     print("-" * 70)
     
     for r in pg_only:
         query_name = r["query_name"][:49]
         result = r["result"]
         if result["status"] == "success":
             time_str = f"{result['time']:.3f}s"
         else:
             time_str = "ERROR"
         print(f"{query_name:<50} {time_str:>15}")
     print("-" * 70)
 
 # Queries solo Spark
 if spark_only:
     print("\n\n⚡ QUERIES OPTIMIZADAS PARA SPARK:")
     print("-" * 70)
     print(f"{'Query':<50} {'Tiempo':>15}")
     print("-" * 70)
     
     for r in spark_only:
         query_name = r["query_name"][:49]
         result = r["result"]
         if result["status"] == "success":
             time_str = f"{result['time']:.3f}s"
         else:
             time_str = "ERROR"
         print(f"{query_name:<50} {time_str:>15}")
     print("-" * 70)

# ============================================================================
# DEFINICIÓN DE QUERIES
# ============================================================================

# Queries originales (se ejecutan en AMBOS motores para comparación)
original_queries = {
 "Query 1 (BASE AGREGACION)": """
     SELECT 
         l_returnflag,
         l_linestatus,
         COUNT(*) as count_order,
         SUM(l_quantity) as sum_qty,
         AVG(l_quantity) as avg_qty
     FROM lineitem
     WHERE l_shipdate <= DATE '1998-12-01'
     GROUP BY l_returnflag, l_linestatus
     ORDER BY l_returnflag, l_linestatus
 """,
 
 "Query 2 (BASE COMPLEX)": """
     SELECT
         c.c_mktsegment,
         COUNT(DISTINCT c.c_custkey) AS total_clientes,
         AVG(c.c_acctbal) AS balance_promedio,
         SUM(l.l_extendedprice * (1 - l.l_discount)) AS facturacion_total
     FROM customer c
     JOIN orders o ON c.c_custkey = o.o_custkey
     JOIN lineitem l ON o.o_orderkey = l.l_orderkey
     WHERE c.c_acctbal > 5000
       AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
       AND l.l_returnflag = 'N'
       AND c.c_custkey IN (
             SELECT c2.c_custkey
             FROM customer c2
             WHERE c2.c_acctbal > (
                 SELECT AVG(c3.c_acctbal)
                 FROM customer c3
             )
       )
     GROUP BY c.c_mktsegment
     HAVING COUNT(DISTINCT o.o_orderkey) > 10
     ORDER BY facturacion_total DESC
 """
}

# Queries optimizadas SOLO para PostgreSQL
optimized_pg_queries = {
 "Query 1 GPT-5 (PG)": """
     SELECT
         l_returnflag,
         l_linestatus,
         COUNT(*) AS count_order,
         SUM(l_quantity) AS sum_qty,
         AVG(l_quantity) AS avg_qty
     FROM lineitem
     WHERE l_shipdate < DATE '1998-12-02'
     GROUP BY 1, 2
     ORDER BY 1, 2
 """,
 
 "Query 2 GPT-5 (PG)": """
     SELECT
         c.c_mktsegment,
         COUNT(DISTINCT c.c_custkey) AS total_clientes,
         AVG(c.c_acctbal) AS balance_promedio,
         SUM(l.l_extendedprice * (1 - l.l_discount)) AS facturacion_total
     FROM customer c
     JOIN (
         SELECT AVG(c_acctbal) AS avg_acctbal
         FROM customer
     ) avg_c ON c.c_acctbal > avg_c.avg_acctbal
     JOIN orders o ON c.c_custkey = o.o_custkey
     JOIN lineitem l ON o.o_orderkey = l.l_orderkey
     WHERE c.c_acctbal > 5000
       AND o.o_orderdate >= DATE '1995-01-01'
       AND o.o_orderdate < DATE '1997-01-01'
       AND l.l_returnflag = 'N'
     GROUP BY 1
     HAVING COUNT(DISTINCT o.o_orderkey) > 10
     ORDER BY 4 DESC
 """,
 
 "Query 1 GEMINI (PG)": """
     SELECT
         l_returnflag,
         l_linestatus,
         COUNT(*) as count_order,
         SUM(l_quantity) as sum_qty,
         AVG(l_quantity) as avg_qty
     FROM lineitem
     WHERE l_shipdate <= DATE '1998-12-01'
     GROUP BY l_returnflag, l_linestatus
     ORDER BY l_returnflag, l_linestatus
 """,
 
 "Query 2 GEMINI (PG)": """
     WITH CustomerAvgBalance AS (
         SELECT AVG(c_acctbal) AS avg_bal
         FROM customer
     ),
     FilteredCustomers AS (
         SELECT 
             c.c_custkey,
             c.c_mktsegment,
             c.c_acctbal
         FROM customer c
         JOIN CustomerAvgBalance cab ON c.c_acctbal > cab.avg_bal
         WHERE c.c_acctbal > 5000
     ),
     GroupedData AS (
         SELECT
             fc.c_mktsegment,
             COUNT(DISTINCT fc.c_custkey) AS total_clientes,
             AVG(fc.c_acctbal) AS balance_promedio,
             COUNT(DISTINCT o.o_orderkey) AS distinct_orders_count,
             SUM(l.l_extendedprice * (1 - l.l_discount)) AS facturacion_total
         FROM FilteredCustomers fc
         JOIN orders o ON fc.c_custkey = o.o_custkey
         JOIN lineitem l ON o.o_orderkey = l.l_orderkey
         WHERE o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
           AND l.l_returnflag = 'N'
         GROUP BY fc.c_mktsegment
     )
     SELECT
         c_mktsegment,
         total_clientes,
         balance_promedio,
         facturacion_total
     FROM GroupedData
     WHERE distinct_orders_count > 10
     ORDER BY facturacion_total DESC
 """,
 "Query 1 CLAUDE (PG)": """
     SELECT 
         l_returnflag,
         l_linestatus,
         COUNT(*) as count_order,
         SUM(l_quantity) as sum_qty,
         SUM(l_quantity) / COUNT(*) as avg_qty
     FROM lineitem
     WHERE l_shipdate < '1998-12-02'::date
     GROUP BY 1, 2
     ORDER BY 1, 2
 """,
 
 "Query 2 CLAUDE (PG)": """
WITH avg_balance AS MATERIALIZED (
    SELECT AVG(c_acctbal) as avg_bal
    FROM customer
    WHERE c_acctbal > 5000
),
qualified_customers AS MATERIALIZED (
    SELECT 
        c.c_custkey,
        c.c_mktsegment,
        c.c_acctbal
    FROM customer c
    CROSS JOIN avg_balance ab
    WHERE c.c_acctbal > ab.avg_bal
      AND c.c_acctbal > 5000
),
order_revenue AS (
    SELECT 
        o.o_custkey,
        o.o_orderkey,
        SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue
    FROM orders o
    INNER JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    WHERE o.o_orderdate >= '1995-01-01'::date
      AND o.o_orderdate < '1997-01-01'::date
      AND l.l_returnflag = 'N'
    GROUP BY o.o_custkey, o.o_orderkey
)
SELECT 
    qc.c_mktsegment,
    COUNT(DISTINCT qc.c_custkey) as total_clientes,
    AVG(qc.c_acctbal) as balance_promedio,
    SUM(orv.revenue) as facturacion_total
FROM qualified_customers qc
INNER JOIN order_revenue orv ON qc.c_custkey = orv.o_custkey
GROUP BY qc.c_mktsegment
HAVING COUNT(DISTINCT orv.o_orderkey) > 10
ORDER BY facturacion_total DESC
"""
}

# Queries optimizadas SOLO para Spark
optimized_spark_queries = {
 "Query 1 GPT-5 (Spark)": """
     WITH lineitem_filtered AS (
         SELECT
             l_returnflag,
             l_linestatus,
             l_quantity
         FROM lineitem
         WHERE l_shipdate <= DATE '1998-12-01'
     )
     SELECT
         l_returnflag,
         l_linestatus,
         COUNT(*) AS count_order,
         SUM(l_quantity) AS sum_qty,
         AVG(l_quantity) AS avg_qty
     FROM lineitem_filtered
     GROUP BY l_returnflag, l_linestatus
     ORDER BY l_returnflag, l_linestatus
 """,
 
 "Query 2 GPT-5 (Spark)": """
     WITH global_avg AS (
         SELECT AVG(c_acctbal) AS avg_acctbal
         FROM customer
     ),
     qualified_customers AS (
         SELECT
             c_custkey,
             c_mktsegment,
             c_acctbal
         FROM customer c
         CROSS JOIN global_avg g
         WHERE c.c_acctbal > 5000
           AND c.c_acctbal > g.avg_acctbal
     ),
     filtered_orders AS (
         SELECT
             o_orderkey,
             o_custkey,
             o_orderdate
         FROM orders
         WHERE o_orderdate >= DATE '1995-01-01'
           AND o_orderdate < DATE '1997-01-01'
     ),
     filtered_lineitem AS (
         SELECT
             l_orderkey,
             l_extendedprice,
             l_discount
         FROM lineitem
         WHERE l_returnflag = 'N'
     )
     SELECT
         qc.c_mktsegment,
         COUNT(DISTINCT qc.c_custkey) AS total_clientes,
         AVG(qc.c_acctbal) AS balance_promedio,
         SUM(fl.l_extendedprice * (1 - fl.l_discount)) AS facturacion_total
     FROM qualified_customers qc
     JOIN filtered_orders fo ON qc.c_custkey = fo.o_custkey
     JOIN filtered_lineitem fl ON fo.o_orderkey = fl.l_orderkey
     GROUP BY qc.c_mktsegment
     HAVING COUNT(DISTINCT fo.o_orderkey) > 10
     ORDER BY facturacion_total DESC
 """,
 
 "Query 1 GEMINI (Spark)": """
     SELECT /*+ COALESCE(3) */
         l_returnflag,
         l_linestatus,
         COUNT(*) as count_order,
         SUM(l_quantity) as sum_qty,
         AVG(l_quantity) as avg_qty
     FROM lineitem
     WHERE l_shipdate <= DATE '1998-12-01'
     GROUP BY l_returnflag, l_linestatus
     ORDER BY l_returnflag, l_linestatus
 """,
 
 "Query 2 GEMINI (Spark)": """
     SELECT /*+ REPARTITION(c_mktsegment), BROADCAST(cab) */
         c.c_mktsegment,
         COUNT(DISTINCT c.c_custkey) AS total_clientes,
         AVG(c.c_acctbal) AS balance_promedio,
         SUM(l.l_extendedprice * (1 - l.l_discount)) AS facturacion_total
     FROM customer c
     JOIN (
         SELECT AVG(c_acctbal) AS avg_bal FROM customer
     ) cab ON c.c_acctbal > cab.avg_bal
     JOIN orders o ON c.c_custkey = o.o_custkey
     JOIN lineitem l ON o.o_orderkey = l.l_orderkey
     WHERE c.c_acctbal > 5000
       AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
       AND l.l_returnflag = 'N'
     GROUP BY c.c_mktsegment
     HAVING COUNT(DISTINCT o.o_orderkey) > 10
     ORDER BY facturacion_total DESC
 """,
 
 "Query 1 CLAUDE (Spark)": """
     SELECT /*+ REPARTITION(l_returnflag, l_linestatus) */
         l_returnflag,
         l_linestatus,
         COUNT(1) as count_order,
         SUM(l_quantity) as sum_qty,
         SUM(l_quantity) / COUNT(1) as avg_qty
     FROM lineitem
     WHERE l_shipdate < date'1998-12-02'
     GROUP BY l_returnflag, l_linestatus
     ORDER BY l_returnflag, l_linestatus
 """,
 
 "Query 2 CLAUDE (Spark)": """
WITH avg_balance AS (
    SELECT /*+ BROADCAST(customer) */ AVG(c_acctbal) as avg_bal
    FROM customer
    WHERE c_acctbal > 5000
),
qualified_customers AS (
    SELECT /*+ BROADCAST(ab) */
        c.c_custkey,
        c.c_mktsegment,
        c.c_acctbal
    FROM customer c
    CROSS JOIN avg_balance ab
    WHERE c.c_acctbal > ab.avg_bal
      AND c.c_acctbal > 5000
),
filtered_data AS (
    SELECT 
        o.o_custkey,
        o.o_orderkey,
        l.l_extendedprice * (1 - l.l_discount) as item_revenue
    FROM orders o
    INNER JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    WHERE o.o_orderdate >= date'1995-01-01'
      AND o.o_orderdate < date'1997-01-01'
      AND l.l_returnflag = 'N'
),
customer_orders AS (
    SELECT /*+ BROADCAST(qc) */
        qc.c_mktsegment,
        qc.c_custkey,
        qc.c_acctbal,
        fd.o_orderkey,
        fd.item_revenue
    FROM qualified_customers qc
    INNER JOIN filtered_data fd ON qc.c_custkey = fd.o_custkey
)
SELECT
    c_mktsegment,
    COUNT(DISTINCT c_custkey) as total_clientes,
    AVG(c_acctbal) as balance_promedio,
    SUM(item_revenue) as facturacion_total
FROM customer_orders
GROUP BY c_mktsegment
HAVING COUNT(DISTINCT o_orderkey) > 10
ORDER BY facturacion_total DESC
"""
}

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

if __name__ == "__main__":
 print("="*90)
 print("🚀 BENCHMARK: PostgreSQL vs Apache Spark")
 print("="*90)
 print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
 print(f"Dataset: TPC-H Benchmark")
 print("="*90)
 
 # Inicializar comparador (warmup=True para calentar cachés)
 comparator = EngineComparator(warmup=True)
 all_results = []
 
 # 1. Ejecutar queries originales (comparación directa)
 print("\n" + "█"*90)
 print("█ FASE 1: QUERIES ORIGINALES (Comparación Directa)")
 print("█"*90)
 for name, query in original_queries.items():
     result = comparator.compare_query(query, name)
     all_results.append(result)
     time.sleep(0.5)  # Pequeña pausa entre queries
 
 # 2. Ejecutar queries optimizadas para PostgreSQL
 print("\n" + "█"*90)
 print("█ FASE 2: QUERIES OPTIMIZADAS PARA POSTGRESQL")
 print("█"*90)
 for name, query in optimized_pg_queries.items():
     result = comparator.run_postgres_only(query, name)
     all_results.append(result)
     time.sleep(0.5)
 
 # 3. Ejecutar queries optimizadas para Spark
 print("\n" + "█"*90)
 print("█ FASE 3: QUERIES OPTIMIZADAS PARA SPARK")
 print("█"*90)
 for name, query in optimized_spark_queries.items():
     result = comparator.run_spark_only(query, name)
     all_results.append(result)
     time.sleep(0.5)
 
 # Imprimir resumen final
 print_summary(all_results)
 
 # Cerrar conexiones
 comparator.close()
 
 print("\n" + "="*90)
 print("✅ BENCHMARK COMPLETADO")
 print("="*90)