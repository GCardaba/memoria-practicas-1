#!/usr/bin/env python3
"""
Script para comparar rendimiento PostgreSQL vs Spark
"""

import psycopg2
from pyspark.sql import SparkSession
import time
import os

class EngineComparator:
  def __init__(self):
      # PostgreSQL
      self.pg_conn = psycopg2.connect(
          dbname="tpch_benchmark",
          user=os.getenv("USER"),
          host="localhost"
      )
      
      # Spark
      self.spark = SparkSession.builder \
          .appName("Engine Comparison") \
          .master("local[*]") \
          .config("spark.driver.memory", "4g") \
          .getOrCreate()
      
      self.spark.sparkContext.setLogLevel("ERROR")
      
      # Cargar tablas en Spark
      tables = ["region", "nation", "customer", "supplier", 
                "part", "partsupp", "orders", "lineitem"]
      for table in tables:
          df = self.spark.read.parquet(f"tpch-dbgen/parquet_data/{table}")
          df.createOrReplaceTempView(table)
      
      print("✅ Ambos motores inicializados\n")
  
  def run_postgres(self, query, name="Query"):
      print(f"🐘 PostgreSQL - {name}...", end=" ", flush=True)
      cur = self.pg_conn.cursor()
      
      start = time.time()
      cur.execute(query)
      results = cur.fetchall()
      elapsed = time.time() - start
      
      cur.close()
      print(f"⏱️  {elapsed:.3f}s")
      return {"engine": "PostgreSQL", "time": elapsed, "rows": len(results)}
  
  def run_spark(self, query, name="Query"):
      print(f"⚡ Spark - {name}...", end=" ", flush=True)
      
      start = time.time()
      results = self.spark.sql(query).collect()
      elapsed = time.time() - start
      
      print(f"⏱️  {elapsed:.3f}s")
      return {"engine": "Spark", "time": elapsed, "rows": len(results)}
  
  def compare_query(self, query, name="Query"):
      print(f"\n{'='*60}")
      print(f"📊 {name}")
      print('='*60)
      
      pg_result = self.run_postgres(query, name)
      spark_result = self.run_spark(query, name)
      
      print(f"\n📈 Resultado:")
      if pg_result["time"] < spark_result["time"]:
          speedup = spark_result["time"] / pg_result["time"]
          print(f"   PostgreSQL fue {speedup:.2f}x más rápido")
      else:
          speedup = pg_result["time"] / spark_result["time"]
          print(f"   Spark fue {speedup:.2f}x más rápido")
      
      print(f"   PostgreSQL: {pg_result['time']:.3f}s")
      print(f"   Spark:      {spark_result['time']:.3f}s")
      print(f"   Diferencia: {abs(pg_result['time'] - spark_result['time']):.3f}s")
      
      return {
          "query_name": name,
          "postgres": pg_result,
          "spark": spark_result,
          "speedup": speedup
      }
  
  def close(self):
      self.pg_conn.close()
      self.spark.stop()

# Queries de prueba
original_queries = {
  "Query 1 (TPC-H BASE AGREGACION)": """
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
  "Query 2 (TPC-H BASE COMPLEX)": """
      SELECT
    c.c_mktsegment,
    COUNT(DISTINCT c.c_custkey) AS total_clientes,
    AVG(c.c_acctbal) AS balance_promedio,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS facturacion_total
FROM customer c
JOIN orders o
    ON c.c_custkey = o.o_custkey
JOIN lineitem l
    ON o.o_orderkey = l.l_orderkey
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
  """,

  
}
optimized_pg_queries = {
    "Query 1 GPT-5 (PG)": """
    SELECT
        l_returnflag,
        l_linestatus,
        COUNT(*)        AS count_order,
        SUM(l_quantity) AS sum_qty,
        AVG(l_quantity) AS avg_qty
    FROM lineitem
    WHERE l_shipdate < DATE '1998-12-02'
    GROUP BY
        1, 2
    ORDER BY
        1, 2
""", 
  
  "Query 2 GPT-5 (PG)": """
  SELECT
      c.c_mktsegment,
      COUNT(DISTINCT c.c_custkey)                 AS total_clientes,
      AVG(c.c_acctbal)                            AS balance_promedio,
      SUM(l.l_extendedprice * (1 - l.l_discount)) AS facturacion_total
  FROM customer c
  JOIN (
      SELECT AVG(c_acctbal) AS avg_acctbal
      FROM customer
  ) avg_c
      ON c.c_acctbal > avg_c.avg_acctbal
  JOIN orders o
      ON c.c_custkey = o.o_custkey
  JOIN lineitem l
      ON o.o_orderkey = l.l_orderkey
  WHERE c.c_acctbal > 5000
    AND o.o_orderdate >= DATE '1995-01-01'
    AND o.o_orderdate <  DATE '1997-01-01'
    AND l.l_returnflag = 'N'
  GROUP BY 1
  HAVING COUNT(DISTINCT o.o_orderkey) > 10
  ORDER BY 4 DESC
""",
  "Query 1 GEMINI (PG) ": """
      SELECT /*+ INDEX(lineitem idx_lineitem_shipdate_returnflag_linestatus) */
          l_returnflag,
          l_linestatus,
          COUNT(*) as count_order,
          SUM(l_quantity) as sum_qty,
          AVG(l_quantity) as avg_qty
      FROM lineitem
      WHERE l_shipdate <= DATE '1998-12-01'
      GROUP BY l_returnflag, l_linestatus
      ORDER BY l_returnflag, l_linestatus;
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
      ORDER BY facturacion_total DESC;
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
INNER JOIN lineitem l USING (o_orderkey)
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
INNER JOIN order_revenue orv USING (o_custkey)
GROUP BY qc.c_mktsegment
HAVING COUNT(DISTINCT orv.o_orderkey) > 10
ORDER BY facturacion_total DESC
"""
}

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
        COUNT(*)        AS count_order,
        SUM(l_quantity) AS sum_qty,
        AVG(l_quantity) AS avg_qty
    FROM lineitem_filtered
    GROUP BY
        l_returnflag,
        l_linestatus
    ORDER BY
        l_returnflag,
        l_linestatus 
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
  WHERE o_oRderdate >= DATE '1995-01-01'
    AND o_oRderdate <  DATE '1997-01-01'
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
  COUNT(DISTINCT qc.c_custkey)                           AS total_clientes,
  AVG(qc.c_acctbal)                                      AS balance_promedio,
  SUM(fl.l_extendedprice * (1 - fl.l_discount))          AS facturacion_total
FROM qualified_customers qc
JOIN filtered_orders  fo ON qc.c_custkey = fo.o_custkey
JOIN filtered_lineitem fl ON fo.o_orderkey = fl.l_orderkey
GROUP BY qc.c_mktsegment
HAVING COUNT(DISTINCT fo.o_orderkey) > 10
ORDER BY facturacion_total DESC"""
,

  "Query 1 GEMINI (Spark) ": """
      SELECT /*+ COALESCE(3) */
          l_returnflag,
          l_linestatus,
          COUNT(*) as count_order,
          SUM(l_quantity) as sum_qty,
          AVG(l_quantity) as avg_qty
      FROM lineitem
      WHERE l_shipdate <= DATE '1998-12-01'
      GROUP BY l_returnflag, l_linestatus
      ORDER BY l_returnflag, l_linestatus;
  """,
  "Query 2 GEMINI (Spark)": """
      SELECT /*+ REPARTITION(c_mktsegment), BROADCAST(c) */
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
      ORDER BY facturacion_total DESC;
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
SELECT AVG(c_acctbal) as avg_bal
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
order_lineitem_filtered AS (
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
order_revenue_agg AS (
SELECT 
    o_custkey,
    o_orderkey,
    SUM(item_revenue) as revenue
FROM order_lineitem_filtered
GROUP BY o_custkey, o_orderkey
)
SELECT /*+ SHUFFLE_REPLICATE_NL(qc) */
qc.c_mktsegment,
COUNT(DISTINCT qc.c_custkey) as total_clientes,
AVG(qc.c_acctbal) as balance_promedio,
SUM(ora.revenue) as facturacion_total
FROM qualified_customers qc
INNER JOIN order_revenue_agg ora ON qc.c_custkey = ora.o_custkey
GROUP BY qc.c_mktsegment
HAVING COUNT(DISTINCT ora.o_orderkey) > 10
ORDER BY facturacion_total DESC
"""
}

# Ejecutar comparaciones
if __name__ == "__main__":
    print("🚀 Comparación de Motores: PostgreSQL vs Spark")
    print("="*60)

    comparator = EngineComparator()
    all_results = []

    # Ejecutar queries originales
    print("\n--- Ejecutando Queries Originales ---")
    for name, query in original_queries.items():
        result = comparator.compare_query(query, name)
        all_results.append(result)

    # Ejecutar queries optimizadas para PostgreSQL
    print("\n--- Ejecutando Queries Optimizadas para PostgreSQL ---")
    for name, query in optimized_pg_queries.items():
        result = comparator.compare_query(query, name)
        all_results.append(result)

    # Ejecutar queries optimizadas para Spark
    print("\n--- Ejecutando Queries Optimizadas para Spark ---")
    for name, query in optimized_spark_queries.items():
        result = comparator.compare_query(query, name)
        all_results.append(result)

    print("\n\n" + "="*60)
    print("📊 RESUMEN FINAL")
    print("="*60)

    for r in all_results:
        print(f"\n{r['query_name']}:")
        print(f"   PostgreSQL: {r['postgres']['time']:.3f}s")
        print(f"   Spark:      {r['spark']['time']:.3f}s")

    comparator.close()
    print("\n✅ Comparación completada")