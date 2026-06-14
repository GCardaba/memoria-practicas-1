#!/usr/bin/env python3
"""
Verifica que queries optimizadas sean semánticamente equivalentes
"""

import psycopg2
from pyspark.sql import SparkSession
import os
from collections import Counter

class SemanticVerifier:
    def __init__(self):
        self.pg_conn = psycopg2.connect(
            dbname="tpch_benchmark",
            user=os.getenv("USER"),
            host="localhost"
        )
        
        self.spark = SparkSession.builder \
            .appName("Semantic Verifier") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        
        for table in ["region", "nation", "customer", "supplier", 
                    "part", "partsupp", "orders", "lineitem"]:
            df = self.spark.read.parquet(f"../tpch-dbgen/parquet_data/{table}")
            df.createOrReplaceTempView(table)

    def execute_pg(self, query: str):
        """Ejecuta query en PostgreSQL y retorna resultados ordenados"""
        cur = self.pg_conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        
        # Ordenar para comparación consistente
        return sorted(results)

    def execute_spark(self, query: str):
        """Ejecuta query en Spark y retorna resultados ordenados"""
        results = self.spark.sql(query).collect()
        
        # Convertir Rows a tuplas y ordenar
        results_tuples = [tuple(row) for row in results]
        return sorted(results_tuples)

    def compare_results(self, results1, results2, tolerance=0.01):
        """
        Compara dos conjuntos de resultados
        
        Args:
            tolerance: Tolerancia para comparación de floats
        """
        if len(results1) != len(results2):
            return False, f"Diferente número de filas: {len(results1)} vs {len(results2)}"
        
        for i, (row1, row2) in enumerate(zip(results1, results2)):
            if len(row1) != len(row2):
                return False, f"Fila {i}: diferente número de columnas"
            
            for j, (val1, val2) in enumerate(zip(row1, row2)):
                # Manejo de valores nulos
                if val1 is None or val2 is None:
                    if val1 != val2:
                        return False, f"Fila {i}, col {j}: {val1} != {val2}"
                    else:
                        continue

                # Intentar comparar como números: redondear a 5 decimales
                try:
                    f1 = float(val1)
                    f2 = float(val2)

                    if round(f1, 5) != round(f2, 5):
                        # Mostrar los valores originales en el mensaje para depuración
                        return False, f"Fila {i}, col {j}: {val1} != {val2} (rounded: {round(f1,5)} != {round(f2,5)})"
                    else:
                        continue
                except (ValueError, TypeError):
                    # No son números: comparar como strings (sin espacios)
                    if str(val1).strip() != str(val2).strip():
                        return False, f"Fila {i}, col {j}: '{val1}' != '{val2}'"
        
        return True, "✅ Resultados idénticos"

    def verify_query_pair(self, query1: str, query2: str, 
                        engine: str, name1: str, name2: str):
        """Verifica que dos queries den el mismo resultado"""
        print(f"\n{'='*70}")
        print(f"🔍 Verificando: {name1} vs {name2} [{engine.upper()}]")
        print('='*70)
        
        try:
            if engine == "postgres":
                results1 = self.execute_pg(query1)
                results2 = self.execute_pg(query2)
            else:
                results1 = self.execute_spark(query1)
                results2 = self.execute_spark(query2)
            
            is_equal, message = self.compare_results(results1, results2)
            
            #print(f"   Filas query 1: {len(results1)}")
            #print(f"   Filas query 2: {len(results2)}")
            print(f"   {message}")
            
            if is_equal:
                print(f"   ✅ SEMÁNTICAMENTE EQUIVALENTES")
            else:
                print(f"   ❌ NO EQUIVALENTES: {message}")
                
                # Mostrar primeras 3 filas de cada resultado
                print(f"\n   Primeras 3 filas de {name1}:")
                for row in results1[:3]:
                    print(f"      {row}")
                
                print(f"\n   Primeras 3 filas de {name2}:")
                for row in results2[:3]:
                    print(f"      {row}")
            
            return is_equal
            
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
            return False

    def close(self):
        self.pg_conn.close()
        self.spark.stop()

# Queries a verificar
query_base = """
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
"""

query_claude_pg = """
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
"""

if __name__ == "__main__":
    verifier = SemanticVerifier()

    # Verificar en PostgreSQL
    verifier.verify_query_pair(
        query_base, query_claude_pg,
        "postgres",
        "BASE", "CLAUDE"
    )

    # Verificar en Spark
    query_claude_spark = """
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
    """

    verifier.verify_query_pair(
        query_base, query_claude_spark,
        "spark",
        "BASE", "CLAUDE"
    )

    verifier.close()

    print("\n✅ Verificación completada")
