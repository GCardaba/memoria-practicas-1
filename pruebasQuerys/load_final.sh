#!/bin/zsh

DB_NAME="tpch_benchmark"

echo "🍎 Carga TPC-H para Mac - SOLUCIÓN FINAL"
echo "========================================"
echo ""

# 1. Recrear base de datos limpia
echo "📦 Recreando base de datos..."
dropdb --if-exists $DB_NAME 2>/dev/null
createdb $DB_NAME
echo ""

# 2. Crear tablas con soporte para trailing pipe
echo "📋 Creando schema..."
psql -d $DB_NAME << 'SQL'

-- REGION
CREATE TABLE region (
  r_regionkey  INTEGER NOT NULL PRIMARY KEY,
  r_name       CHAR(25) NOT NULL,
  r_comment    VARCHAR(152),
  skip         TEXT  -- Para capturar el trailing pipe
);

-- NATION
CREATE TABLE nation (
  n_nationkey  INTEGER NOT NULL PRIMARY KEY,
  n_name       CHAR(25) NOT NULL,
  n_regionkey  INTEGER NOT NULL,
  n_comment    VARCHAR(152),
  skip         TEXT
);

-- CUSTOMER
CREATE TABLE customer (
  c_custkey     INTEGER NOT NULL PRIMARY KEY,
  c_name        VARCHAR(25) NOT NULL,
  c_address     VARCHAR(40) NOT NULL,
  c_nationkey   INTEGER NOT NULL,
  c_phone       CHAR(15) NOT NULL,
  c_acctbal     DECIMAL(15,2) NOT NULL,
  c_mktsegment  CHAR(10) NOT NULL,
  c_comment     VARCHAR(117) NOT NULL,
  skip          TEXT
);

-- SUPPLIER
CREATE TABLE supplier (
  s_suppkey     INTEGER NOT NULL PRIMARY KEY,
  s_name        CHAR(25) NOT NULL,
  s_address     VARCHAR(40) NOT NULL,
  s_nationkey   INTEGER NOT NULL,
  s_phone       CHAR(15) NOT NULL,
  s_acctbal     DECIMAL(15,2) NOT NULL,
  s_comment     VARCHAR(101) NOT NULL,
  skip          TEXT
);

-- PART
CREATE TABLE part (
  p_partkey     INTEGER NOT NULL PRIMARY KEY,
  p_name        VARCHAR(55) NOT NULL,
  p_mfgr        CHAR(25) NOT NULL,
  p_brand       CHAR(10) NOT NULL,
  p_type        VARCHAR(25) NOT NULL,
  p_size        INTEGER NOT NULL,
  p_container   CHAR(10) NOT NULL,
  p_retailprice DECIMAL(15,2) NOT NULL,
  p_comment     VARCHAR(23) NOT NULL,
  skip          TEXT
);

-- PARTSUPP
CREATE TABLE partsupp (
  ps_partkey     INTEGER NOT NULL,
  ps_suppkey     INTEGER NOT NULL,
  ps_availqty    INTEGER NOT NULL,
  ps_supplycost  DECIMAL(15,2) NOT NULL,
  ps_comment     VARCHAR(199) NOT NULL,
  skip           TEXT,
  PRIMARY KEY (ps_partkey, ps_suppkey)
);

-- ORDERS
CREATE TABLE orders (
  o_orderkey       INTEGER NOT NULL PRIMARY KEY,
  o_custkey        INTEGER NOT NULL,
  o_orderstatus    CHAR(1) NOT NULL,
  o_totalprice     DECIMAL(15,2) NOT NULL,
  o_orderdate      DATE NOT NULL,
  o_orderpriority  CHAR(15) NOT NULL,
  o_clerk          CHAR(15) NOT NULL,
  o_shippriority   INTEGER NOT NULL,
  o_comment        VARCHAR(79) NOT NULL,
  skip             TEXT
);

-- LINEITEM
CREATE TABLE lineitem (
  l_orderkey       INTEGER NOT NULL,
  l_partkey        INTEGER NOT NULL,
  l_suppkey        INTEGER NOT NULL,
  l_linenumber     INTEGER NOT NULL,
  l_quantity       DECIMAL(15,2) NOT NULL,
  l_extendedprice  DECIMAL(15,2) NOT NULL,
  l_discount       DECIMAL(15,2) NOT NULL,
  l_tax            DECIMAL(15,2) NOT NULL,
  l_returnflag     CHAR(1) NOT NULL,
  l_linestatus     CHAR(1) NOT NULL,
  l_shipdate       DATE NOT NULL,
  l_commitdate     DATE NOT NULL,
  l_receiptdate    DATE NOT NULL,
  l_shipinstruct   CHAR(25) NOT NULL,
  l_shipmode       CHAR(10) NOT NULL,
  l_comment        VARCHAR(44) NOT NULL,
  skip             TEXT,
  PRIMARY KEY (l_orderkey, l_linenumber)
);

SQL

echo "✅ Schema creado"
echo ""

# 3. Cargar datos
echo "📥 Cargando datos..."
echo ""

load_table() {
  local table=$1
  echo -n "   ${table}..."
  
  START=$(date +%s)
  psql -d $DB_NAME -c "\COPY ${table} FROM '${table}.tbl' DELIMITER '|' CSV;" > /dev/null 2>&1
  
  if [[ $? -eq 0 ]]; then
      COUNT=$(psql -d $DB_NAME -tc "SELECT COUNT(*) FROM ${table}" | tr -d ' ')
      END=$(date +%s)
      DURATION=$((END - START))
      echo " ✅ ${COUNT} filas (${DURATION}s)"
      return 0
  else
      echo " ❌ ERROR"
      return 1
  fi
}

load_table "region" || exit 1
load_table "nation" || exit 1
load_table "supplier" || exit 1
load_table "customer" || exit 1
load_table "part" || exit 1
load_table "partsupp" || exit 1
load_table "orders" || exit 1
load_table "lineitem" || exit 1

echo ""

# 4. Eliminar columna 'skip' de todas las tablas
echo "🧹 Eliminando columnas temporales..."
psql -d $DB_NAME << 'SQL' > /dev/null 2>&1
ALTER TABLE region DROP COLUMN skip;
ALTER TABLE nation DROP COLUMN skip;
ALTER TABLE customer DROP COLUMN skip;
ALTER TABLE supplier DROP COLUMN skip;
ALTER TABLE part DROP COLUMN skip;
ALTER TABLE partsupp DROP COLUMN skip;
ALTER TABLE orders DROP COLUMN skip;
ALTER TABLE lineitem DROP COLUMN skip;
SQL

echo ""

# 5. Agregar foreign keys e índices
echo "🔗 Creando relaciones e índices..."
psql -d $DB_NAME << 'SQL' > /dev/null 2>&1

-- Foreign keys
ALTER TABLE nation ADD FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey);
ALTER TABLE supplier ADD FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey);
ALTER TABLE customer ADD FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey);
ALTER TABLE partsupp ADD FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey);
ALTER TABLE partsupp ADD FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey);
ALTER TABLE orders ADD FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey);
ALTER TABLE lineitem ADD FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey);
ALTER TABLE lineitem ADD FOREIGN KEY (l_partkey) REFERENCES part(p_partkey);
ALTER TABLE lineitem ADD FOREIGN KEY (l_suppkey) REFERENCES supplier(s_suppkey);

-- Índices
CREATE INDEX idx_customer_nationkey ON customer(c_nationkey);
CREATE INDEX idx_supplier_nationkey ON supplier(s_nationkey);
CREATE INDEX idx_nation_regionkey ON nation(n_regionkey);
CREATE INDEX idx_orders_custkey ON orders(o_custkey);
CREATE INDEX idx_orders_orderdate ON orders(o_orderdate);
CREATE INDEX idx_lineitem_orderkey ON lineitem(l_orderkey);
CREATE INDEX idx_lineitem_partkey ON lineitem(l_partkey);
CREATE INDEX idx_lineitem_suppkey ON lineitem(l_suppkey);
CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate);
CREATE INDEX idx_partsupp_partkey ON partsupp(ps_partkey);
CREATE INDEX idx_partsupp_suppkey ON partsupp(ps_suppkey);

SQL

echo ""

# 6. Analizar para estadísticas
echo "🔍 Recolectando estadísticas..."
psql -d $DB_NAME -c "ANALYZE;" > /dev/null 2>&1

echo ""
echo "✅ ¡CARGA COMPLETADA CON ÉXITO!"
echo ""
echo "📊 Resumen de datos cargados:"
echo ""

psql -d $DB_NAME << 'SQL'
\pset border 2
SELECT 
  table_name as "Tabla",
  pg_size_pretty(pg_total_relation_size('public.'||table_name)) AS "Tamaño",
  (xpath('/row/cnt/text()', xml_count))[1]::text::int as "Filas"
FROM (
  SELECT table_name, 
         query_to_xml(format('SELECT COUNT(*) AS cnt FROM %I', table_name), false, true, '') as xml_count
  FROM information_schema.tables
  WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
) t
ORDER BY pg_total_relation_size('public.'||table_name) DESC;
SQL

echo ""
echo "🧪 Test de consulta:"
echo ""

psql -d $DB_NAME << 'SQL'
\timing on
SELECT 
  r.r_name as region,
  COUNT(DISTINCT n.n_nationkey) as paises,
  COUNT(DISTINCT c.c_custkey) as clientes
FROM region r
JOIN nation n ON r.r_regionkey = n.n_regionkey
JOIN customer c ON n.n_nationkey = c.c_nationkey
GROUP BY r.r_name
ORDER BY clientes DESC;
SQL

echo ""
echo "🎉 Base de datos 'tpch_benchmark' lista para usar!"
echo ""
echo "Conectar con: psql -d tpch_benchmark"
echo ""

