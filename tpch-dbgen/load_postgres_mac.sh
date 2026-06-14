#!/bin/zsh

# Configuración
DB_NAME="tpch_benchmark"

echo "🍎 Script de carga TPC-H para Mac"
echo "=================================="
echo ""

# Verificar que PostgreSQL está corriendo
if ! pg_isready -q; then
echo "❌ PostgreSQL no está corriendo"
echo "   Ejecuta: brew services start postgresql@16"
exit 1
fi

echo "✅ PostgreSQL está corriendo"
echo ""

# Verificar archivos .tbl
echo "🔍 Verificando archivos .tbl..."
TABLES=(region nation customer supplier part partsupp orders lineitem)
MISSING=0

for table in $TABLES; do
if [[ ! -f "${table}.tbl" ]]; then
    echo "   ❌ Falta: ${table}.tbl"
    MISSING=$((MISSING + 1))
else
    echo "   ✅ ${table}.tbl"
fi
done

if [[ $MISSING -gt 0 ]]; then
echo ""
echo "❌ Faltan $MISSING archivos .tbl"
echo "   Ejecuta: ./dbgen -s 1"
exit 1
fi

echo ""
echo "📋 Creando schema..."
psql -d $DB_NAME -f create_schema.sql > /dev/null 2>&1

echo ""
echo "📥 Cargando datos..."
echo "   (Esto tomará 5-10 minutos en Mac)"
echo ""

# Función para cargar con barra de progreso
load_table() {
local table=$1
local file="${table}.tbl"
local lines=$(wc -l < "$file" | tr -d ' ')

echo -n "   → ${table}... "

START=$(date +%s)
psql -d $DB_NAME -c "\COPY ${table} FROM '${file}' DELIMITER '|' CSV;" > /dev/null 2>&1
END=$(date +%s)
DURATION=$((END - START))

echo "✅ (${lines} filas en ${DURATION}s)"
}

# Cargar tablas en orden (pequeñas primero por foreign keys)
load_table "region"
load_table "nation"
load_table "supplier"
load_table "customer"
load_table "part"
load_table "partsupp"
load_table "orders"
load_table "lineitem"

echo ""
echo "🔍 Recolectando estadísticas (ANALYZE)..."
psql -d $DB_NAME -c "ANALYZE VERBOSE;" 2>&1 | grep "analyzing" | sed 's/^/   /'

echo ""
echo "✅ ¡Carga completada!"
echo ""
echo "📊 Resumen de tablas:"
echo ""

psql -d $DB_NAME -t << 'SQL'
SELECT 
'   ' || 
rpad(tablename::text, 15) || 
rpad(pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)), 12) || 
to_char(n_live_tup, '999,999,999') || ' filas'
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
SQL

echo ""
echo "🧪 Prueba rápida:"
psql -d $DB_NAME << 'SQL'
SELECT 
(SELECT COUNT(*) FROM customer) as customers,
(SELECT COUNT(*) FROM orders) as orders,
(SELECT COUNT(*) FROM lineitem) as lineitems;
SQL

echo ""
echo "🎯 Siguiente paso: Instalar Spark"
echo "   brew install apache-spark"
