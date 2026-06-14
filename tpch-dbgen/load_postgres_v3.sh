#!/bin/zsh

DB_NAME="tpch_benchmark"

echo "🍎 TPC-H Carga v3 (con manejo de trailing pipe)"
echo "================================================"
echo ""

# Recrear base de datos
dropdb --if-exists $DB_NAME
createdb $DB_NAME

# Crear schema con tablas _raw
psql -d $DB_NAME -f create_schema_fixed.sql

echo "📥 Cargando datos en tablas temporales..."
echo ""

load_and_transfer() {
local table=$1
local file="${table}.tbl"

echo -n "   ${table}..."

# Cargar en tabla_raw
psql -d $DB_NAME -c "\COPY ${table}_raw FROM '${file}' DELIMITER '|' CSV;" 2>&1 | grep -v "COPY"

# Transferir a tabla final (sin columna skip)
psql -d $DB_NAME -c "INSERT INTO ${table} SELECT * FROM ${table}_raw WHERE ${table}_raw.skip IS NULL OR ${table}_raw.skip = '';" > /dev/null 2>&1

# Contar
COUNT=$(psql -d $DB_NAME -tc "SELECT COUNT(*) FROM ${table}" | tr -d ' ')

echo " ✅ ${COUNT} filas"
}

# Cargar todas las tablas
load_and_transfer "region"
load_and_transfer "nation"
load_and_transfer "supplier"
load_and_transfer "customer"
load_and_transfer "part"
load_and_transfer "partsupp"
load_and_transfer "orders"
load_and_transfer "lineitem"

echo ""
echo "🗑️  Limpiando tablas temporales..."
psql -d $DB_NAME -c "DROP TABLE region_raw, nation_raw, supplier_raw, customer_raw, part_raw, partsupp_raw, orders_raw, lineitem_raw;" > /dev/null 2>&1

echo "🔍 Analizando..."
psql -d $DB_NAME -c "ANALYZE;" > /dev/null 2>&1

echo ""
echo "✅ CARGA COMPLETADA"
echo ""
echo "📊 Verificación:"
psql -d $DB_NAME << 'SQL'
SELECT 
'region' as tabla, COUNT(*) as filas FROM region
UNION ALL SELECT 'nation', COUNT(*) FROM nation
UNION ALL SELECT 'customer', COUNT(*) FROM customer
UNION ALL SELECT 'supplier', COUNT(*) FROM supplier
UNION ALL SELECT 'part', COUNT(*) FROM part
UNION ALL SELECT 'partsupp', COUNT(*) FROM partsupp
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'lineitem', COUNT(*) FROM lineitem;
SQL
