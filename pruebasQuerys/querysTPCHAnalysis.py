#!/usr/bin/env python3
"""
Benchmark PostgreSQL vs Spark con múltiples iteraciones y visualizaciones
"""

import psycopg2
from pyspark.sql import SparkSession
import time
import os
import sys
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# Configuración de estilo para gráficos
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

class BenchmarkRunner:
    def __init__(self, num_iterations: int = 5, warmup: bool = True):
        """
        Inicializa el benchmark con múltiples iteraciones
        
        Args:
            num_iterations: Número de veces que se ejecuta cada query
            warmup: Si ejecutar queries de calentamiento
        """
        self.num_iterations = num_iterations
        self.results = defaultdict(list)
        
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
            print(f"   ❌ Error PostgreSQL: {e}")
            sys.exit(1)
        
        # Spark
        try:
            self.spark = SparkSession.builder \
                .appName("Benchmark") \
                .master("local[*]") \
                .config("spark.driver.memory", "4g") \
                .config("spark.sql.shuffle.partitions", "200") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("ERROR")
            print("   ✅ Spark inicializado")
        except Exception as e:
            print(f"   ❌ Error Spark: {e}")
            sys.exit(1)
        
        # Cargar tablas en Spark
        print("   📦 Cargando tablas TPC-H en Spark...")
        tables = ["region", "nation", "customer", "supplier", 
                "part", "partsupp", "orders", "lineitem"]
        
        for table in tables:
            df = self.spark.read.parquet(f"tpch-dbgen/parquet_data/{table}")
            df.createOrReplaceTempView(table)
            print(f"      • {table}")
        
        print(f"\n✅ Motores listos | Iteraciones: {num_iterations}\n")
        
        if warmup:
            self._warmup()

    def _warmup(self):
        """Calentar cachés"""
        print("🔥 Calentando cachés...")
        warmup_query = "SELECT COUNT(*) FROM lineitem"
        
        cur = self.pg_conn.cursor()
        cur.execute(warmup_query)
        cur.fetchall()
        cur.close()
        
        self.spark.sql(warmup_query).collect()
        print("   ✅ Cachés calientes\n")

    def run_query_multiple_times(
        self, 
        query: str, 
        engine: str, 
        query_name: str
    ) -> Dict:
        """
        Ejecuta una query N veces y calcula estadísticas
        
        Args:
            query: Query SQL
            engine: 'postgres' o 'spark'
            query_name: Nombre identificador
            
        Returns:
            Dict con estadísticas de tiempo
        """
        times = []
        errors = 0
        
        print(f"   {'🐘' if engine == 'postgres' else '⚡'} {query_name} ({engine}):", end=" ")
        
        for i in range(self.num_iterations):
            try:
                if engine == 'postgres':
                    cur = self.pg_conn.cursor()
                    start = time.time()
                    cur.execute(query)
                    cur.fetchall()
                    elapsed = time.time() - start
                    cur.close()
                else:  # spark
                    start = time.time()
                    self.spark.sql(query).collect()
                    elapsed = time.time() - start
                
                times.append(elapsed)
                print(f".", end="", flush=True)
            except Exception as e:
                errors += 1
                print(f"✗", end="", flush=True)
        
        print()
        
        if len(times) == 0:
            return {
                "query_name": query_name,
                "engine": engine,
                "status": "error",
                "mean": None,
                "std": None,
                "min": None,
                "max": None,
                "times": []
            }
        
        return {
            "query_name": query_name,
            "engine": engine,
            "status": "success",
            "mean": np.mean(times),
            "std": np.std(times),
            "min": np.min(times),
            "max": np.max(times),
            "median": np.median(times),
            "times": times,
            "iterations": len(times),
            "errors": errors
        }

    def run_benchmark(self, queries: Dict[str, str], phase_name: str):
        """
        Ejecuta todas las queries de una fase
        
        Args:
            queries: Dict de {nombre: query_sql}
            phase_name: Nombre de la fase
        """
        print(f"\n{'='*70}")
        print(f"📊 {phase_name}")
        print('='*70)
        
        for name, query in queries.items():
            # Determinar motor según nombre
            if "(PG)" in name or "BASE" in name:
                if "BASE" in name:
                    # Ejecutar en ambos motores
                    result_pg = self.run_query_multiple_times(
                        query, "postgres", name
                    )
                    result_spark = self.run_query_multiple_times(
                        query, "spark", name
                    )
                    self.results[name].append(result_pg)
                    self.results[name].append(result_spark)
                else:
                    # Solo PostgreSQL
                    result = self.run_query_multiple_times(
                        query, "postgres", name
                    )
                    self.results[name].append(result)
            elif "(Spark)" in name:
                # Solo Spark
                result = self.run_query_multiple_times(
                    query, "spark", name
                )
                self.results[name].append(result)
            
            time.sleep(0.3)

    def generate_statistics_table(self) -> pd.DataFrame:
        """Genera DataFrame con todas las estadísticas"""
        data = []
        
        for query_name, results_list in self.results.items():
            for result in results_list:
                if result["status"] == "success":
                    data.append({
                        "Query": query_name,
                        "Motor": result["engine"],
                        "Media (s)": result["mean"],
                        "Std Dev (s)": result["std"],
                        "Min (s)": result["min"],
                        "Max (s)": result["max"],
                        "Mediana (s)": result["median"],
                        "Iteraciones": result["iterations"]
                    })
        
        return pd.DataFrame(data)

    def create_visualizations(self, output_dir: str = "visualizations"):
        """Genera todas las visualizaciones"""
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\n📈 Generando visualizaciones en ./{output_dir}/")
        
        # 1. Comparación Query 1 (todas variantes)
        self._plot_query_comparison(1, output_dir)
        
        # 2. Comparación Query 2 (todas variantes)
        self._plot_query_comparison(2, output_dir)
        
        # 3. Mejoras relativas vs baseline
        self._plot_improvements(output_dir)
        
        # 4. Gráfico de cajas (box plot)
        self._plot_boxplots(output_dir)
        
        # 5. Heatmap de rendimiento
        self._plot_heatmap(output_dir)
        
        # 6. Tabla de estadísticas
        self._export_statistics_table(output_dir)
        
        print(f"✅ Visualizaciones guardadas en ./{output_dir}/\n")

    def _plot_query_comparison(self, query_num: int, output_dir: str):
        """Gráfico comparativo para Query 1 o Query 2"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Filtrar resultados de la query específica
        query_pattern = f"Query {query_num}"
        
        pg_data = {}
        spark_data = {}
        
        for query_name, results_list in self.results.items():
            if query_pattern in query_name:
                for result in results_list:
                    if result["status"] == "success":
                        label = query_name.split("(")[0].strip()
                        
                        if "BASE" in query_name:
                            label = "BASE"
                        elif "GPT-5" in query_name:
                            label = "GPT-5"
                        elif "GEMINI" in query_name:
                            label = "Gemini"
                        elif "CLAUDE" in query_name:
                            label = "Claude"
                        
                        if result["engine"] == "postgres":
                            pg_data[label] = result
                        else:
                            spark_data[label] = result
        
        # PostgreSQL
        if pg_data:
            labels = list(pg_data.keys())
            means = [pg_data[l]["mean"] for l in labels]
            stds = [pg_data[l]["std"] for l in labels]
            
            colors = ['#FF6B6B' if l == 'BASE' else '#4ECDC4' for l in labels]
            bars = ax1.bar(labels, means, yerr=stds, capsize=5, 
                        color=colors, alpha=0.8, edgecolor='black')
            
            # Añadir valores encima de barras
            for bar, mean in zip(bars, means):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height,
                        f'{mean:.3f}s',
                        ha='center', va='bottom', fontweight='bold')
            
            ax1.set_title(f'PostgreSQL - Query {query_num}', 
                        fontsize=14, fontweight='bold')
            ax1.set_ylabel('Tiempo Medio (segundos)', fontsize=12)
            ax1.set_xlabel('Variante', fontsize=12)
            ax1.grid(axis='y', alpha=0.3)
        
        # Spark
        if spark_data:
            labels = list(spark_data.keys())
            means = [spark_data[l]["mean"] for l in labels]
            stds = [spark_data[l]["std"] for l in labels]
            
            colors = ['#FF6B6B' if l == 'BASE' else '#95E1D3' for l in labels]
            bars = ax2.bar(labels, means, yerr=stds, capsize=5,
                        color=colors, alpha=0.8, edgecolor='black')
            
            for bar, mean in zip(bars, means):
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height,
                        f'{mean:.3f}s',
                        ha='center', va='bottom', fontweight='bold')
            
            ax2.set_title(f'Spark - Query {query_num}', 
                        fontsize=14, fontweight='bold')
            ax2.set_ylabel('Tiempo Medio (segundos)', fontsize=12)
            ax2.set_xlabel('Variante', fontsize=12)
            ax2.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/query_{query_num}_comparison.png', 
                dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   ✓ query_{query_num}_comparison.png")

    def _plot_improvements(self, output_dir: str):
        """Gráfico de mejoras porcentuales respecto a baseline"""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        queries = [1, 2]
        engines = ["postgres", "spark"]
        
        for idx, (q, eng) in enumerate([(1, "postgres"), (1, "spark"), 
                                        (2, "postgres"), (2, "spark")]):
            ax = axes[idx // 2, idx % 2]
            
            query_pattern = f"Query {q}"
            
            # Obtener tiempo base
            baseline_time = None
            for query_name, results_list in self.results.items():
                if f"Query {q} (BASE" in query_name:
                    for result in results_list:
                        if result["engine"] == eng and result["status"] == "success":
                            baseline_time = result["mean"]
                            break
            
            if baseline_time is None:
                continue
            
            # Calcular mejoras
            improvements = {}
            for query_name, results_list in self.results.items():
                if query_pattern in query_name and "BASE" not in query_name:
                    for result in results_list:
                        if result["engine"] == eng and result["status"] == "success":
                            llm = ""
                            if "GPT-5" in query_name:
                                llm = "GPT-5"
                            elif "GEMINI" in query_name:
                                llm = "Gemini"
                            elif "CLAUDE" in query_name:
                                llm = "Claude"
                            
                            if llm and eng in query_name or (eng == "postgres" and "(PG)" in query_name) or (eng == "spark" and "(Spark)" in query_name):
                                improvement = ((baseline_time - result["mean"]) / baseline_time) * 100
                                improvements[llm] = improvement
            
            if improvements:
                llms = list(improvements.keys())
                values = list(improvements.values())
                colors = ['#2ECC71' if v > 0 else '#E74C3C' for v in values]
                
                bars = ax.barh(llms, values, color=colors, alpha=0.8, edgecolor='black')
                
                for bar, val in zip(bars, values):
                    width = bar.get_width()
                    ax.text(width, bar.get_y() + bar.get_height()/2.,
                        f'{val:+.1f}%',
                        ha='left' if val > 0 else 'right',
                        va='center', fontweight='bold')
                
                ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
                ax.set_xlabel('Mejora vs Baseline (%)', fontsize=11)
                ax.set_title(f'Query {q} - {eng.upper()}\n(Baseline: {baseline_time:.3f}s)', 
                        fontsize=12, fontweight='bold')
                ax.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/improvements_summary.png', 
                dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   ✓ improvements_summary.png")

    def _plot_boxplots(self, output_dir: str):
        """Box plots para visualizar distribución de tiempos"""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        for idx, (q, eng) in enumerate([(1, "postgres"), (1, "spark"), 
                                        (2, "postgres"), (2, "spark")]):
            ax = axes[idx // 2, idx % 2]
            
            query_pattern = f"Query {q}"
            
            data_to_plot = []
            labels = []
            
            for query_name, results_list in self.results.items():
                if query_pattern in query_name:
                    for result in results_list:
                        if result["engine"] == eng and result["status"] == "success":
                            label = ""
                            if "BASE" in query_name:
                                label = "BASE"
                            elif "GPT-5" in query_name:
                                label = "GPT-5"
                            elif "GEMINI" in query_name:
                                label = "Gemini"
                            elif "CLAUDE" in query_name:
                                label = "Claude"
                            
                            if label and (eng == "postgres" and "(PG)" in query_name or 
                                        eng == "spark" and "(Spark)" in query_name or
                                        "BASE" in query_name):
                                data_to_plot.append(result["times"])
                                labels.append(label)
            
            if data_to_plot:
                bp = ax.boxplot(data_to_plot, labels=labels, patch_artist=True,
                            showmeans=True, meanline=True)
                
                for patch in bp['boxes']:
                    patch.set_facecolor('#B8E6F0')
                    patch.set_alpha(0.7)
                
                ax.set_title(f'Query {q} - {eng.upper()}', 
                        fontsize=12, fontweight='bold')
                ax.set_ylabel('Tiempo (segundos)', fontsize=11)
                ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/boxplots_distribution.png', 
                dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   ✓ boxplots_distribution.png")

    def _plot_heatmap(self, output_dir: str):
        """Heatmap de rendimiento: LLM x Motor"""
        # Preparar datos
        data_matrix = []
        row_labels = []
        
        for q in [1, 2]:
            for eng in ["postgres", "spark"]:
                query_pattern = f"Query {q}"
                row_label = f"Q{q}-{eng[:2].upper()}"
                row_data = []
                
                # Baseline
                baseline = None
                for query_name, results_list in self.results.items():
                    if f"Query {q} (BASE" in query_name:
                        for result in results_list:
                            if result["engine"] == eng:
                                baseline = result["mean"]
                                break
                
                row_data.append(baseline if baseline else np.nan)
                
                # LLMs
                for llm in ["GPT-5", "GEMINI", "CLAUDE"]:
                    found = False
                    for query_name, results_list in self.results.items():
                        if query_pattern in query_name and llm in query_name:
                            for result in results_list:
                                if result["engine"] == eng and result["status"] == "success":
                                    if (eng == "postgres" and "(PG)" in query_name) or \
                                    (eng == "spark" and "(Spark)" in query_name):
                                        row_data.append(result["mean"])
                                        found = True
                                        break
                            if found:
                                break
                    
                    if not found:
                        row_data.append(np.nan)
                
                data_matrix.append(row_data)
                row_labels.append(row_label)
        
        df_heatmap = pd.DataFrame(
            data_matrix,
            columns=["BASE", "GPT-5", "Gemini", "Claude"],
            index=row_labels
        )
        
        plt.figure(figsize=(10, 8))
        sns.heatmap(df_heatmap, annot=True, fmt='.3f', cmap='RdYlGn_r',
                cbar_kws={'label': 'Tiempo (segundos)'},
                linewidths=1, linecolor='black')
        
        plt.title('Heatmap de Rendimiento: LLM x Motor x Query', 
                fontsize=14, fontweight='bold', pad=20)
        plt.xlabel('Variante / LLM', fontsize=12)
        plt.ylabel('Query - Motor', fontsize=12)
        plt.tight_layout()
        
        plt.savefig(f'{output_dir}/heatmap_performance.png', 
                dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   ✓ heatmap_performance.png")

    def _export_statistics_table(self, output_dir: str):
        """Exporta tabla de estadísticas"""
        df = self.generate_statistics_table()
        
        # CSV
        csv_path = f'{output_dir}/statistics.csv'
        df.to_csv(csv_path, index=False)
        print(f"   ✓ statistics.csv")
        
        # JSON
        json_path = f'{output_dir}/results.json'
        with open(json_path, 'w') as f:
            json.dump(dict(self.results), f, indent=2, default=str)
        print(f"   ✓ results.json")
        
        # Tabla formateada como imagen
        fig, ax = plt.subplots(figsize=(14, len(df) * 0.4 + 2))
        ax.axis('tight')
        ax.axis('off')
        
        table_data = df.round(3).values.tolist()
        table = ax.table(cellText=table_data, 
                        colLabels=df.columns,
                        cellLoc='center',
                        loc='center',
                        colWidths=[0.3, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1])
        
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 2)
        
        # Colorear header
        for i in range(len(df.columns)):
            table[(0, i)].set_facecolor('#4ECDC4')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        plt.title('Estadísticas Completas del Benchmark', 
                fontsize=14, fontweight='bold', pad=20)
        plt.savefig(f'{output_dir}/statistics_table.png', 
                dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"   ✓ statistics_table.png")

    def close(self):
        """Cerrar conexiones"""
        self.pg_conn.close()
        self.spark.stop()

# ============================================================================
# QUERIES (las mismas que antes)
# ============================================================================

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
    print("🚀 BENCHMARK CON MÚLTIPLES ITERACIONES Y VISUALIZACIONES")
    print("="*90)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*90)

    # Configuración
    NUM_ITERATIONS = 5  # Cambia a 10 para más precisión

    # Inicializar
    runner = BenchmarkRunner(num_iterations=NUM_ITERATIONS, warmup=True)

    # Ejecutar benchmarks
    runner.run_benchmark(original_queries, "QUERIES BASE")
    runner.run_benchmark(optimized_pg_queries, "QUERIES OPTIMIZADAS POSTGRESQL")
    runner.run_benchmark(optimized_spark_queries, "QUERIES OPTIMIZADAS SPARK")

    # Generar visualizaciones
    runner.create_visualizations()

    # Mostrar tabla de estadísticas
    df_stats = runner.generate_statistics_table()
    print("\n" + "="*90)
    print("📊 TABLA DE ESTADÍSTICAS")
    print("="*90)
    print(df_stats.to_string(index=False))

    # Cerrar
    runner.close()

    print("\n" + "="*90)
    print("✅ BENCHMARK COMPLETADO")
    print("📁 Resultados guardados en: ./visualizations/")
    print("="*90)