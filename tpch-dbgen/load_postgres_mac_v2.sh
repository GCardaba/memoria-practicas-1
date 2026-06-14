#!/bin/zsh

DB_NAME="tpch_benchmark"

echo "🍎 Script de carga TPC-H para Mac (v2)"
echo "======================================="
echo ""

# 1. Verificar PostgreSQL
if ! pg_isready -q; then
echo "❌ PostgreSQL no está corriendo"
echo "   Ejecuta: brew services start postgresql@16"
exit 1
fi

echo "✅ PostgreSQL corriendo"
echo ""

# 2. Verificar archivos .tbl
echo "🔍 Verificando archivos .tbl..."
TABLES=(region nation customer supplier part partsupp orders lineitem)
MISSING=0

for table in $TABLES; do
if [[ ! -f "${table}.tbl" ]]; then
    echo "   ❌ Falta: ${table}.tbl"
    MISSING=$((MISSING + 1))
fi
done

if [[ $MISSING -gt 0 ]]; then
echo "❌ Faltan $MISSING archivos .tbl"
exit 1
fi

echo "✅ Todos los archivos .tbl presentes"
echo ""

# 3. CREAR la base de datos (esto faltaba!)
echo "📦 Creando base de datos '$DB_NAME'..."

# Eliminar si existe
dropdb --if-exists $DB_NAME 2>/dev/null

# Crear nueva
createdb $DB_NAME

if [[ $? -ne 0 ]]; then
echo "❌ Error al crear base de datos"
exit 1
fi

echo "✅ Base de datos creada"
echo ""

# 4. Crear schema
echo "📋 Creando tablas..."
psql -d $DB_NAME -f create_schema.sql > /dev/null 2>&1

if [[ $? -ne 0 ]]; then
echo "❌ Error al crear schema"
echo "   Revisa create_schema.sql"
exit 1
fi

echo "✅ Tablas creadas"
echo ""

# 5. Cargar datos
echo "📥 Cargando datos (5-10 minutos)..."
echo ""

load_table() {
local table=$1
local file="${table}.tbl"

echo -n "   ${table}..."

START=$(date +%s)
psql -d $DB_NAME -c "\COPY ${table} FROM '${file}' DELIMITER '|' CSV;" > /dev/null 2>&1
END=$(date +%s)

if [[ $? -eq 0 ]]; then
    DURATION=$((END - START))
    ROWS=$(psql -d $DB_NAME -tc "SELECT COUNT(*) FROM ${table}" | tr -d ' ')
    echo " ✅ ${ROWS} filas (${DURATION}s)"
else
    echo " ❌ ERROR"
    return 1
fi
}

# Cargar en orden
load_table "region" || exit 1
load_table "nation" || exit 1
load_table "supplier" || exit 1
load_table "customer" || exit 1
load_table "part" || exit 1
load_table "partsupp" || exit 1
load_table "orders" || exit 1
load_table "lineitem" || exit 1

echo ""
echo "🔍 Analizando estadísticas..."
psql -d $DB_NAME -c "ANALYZE;" > /dev/null 2>&1

echo ""
echo "✅ ¡CARGA COMPLETADA!"
echo ""
echo "📊 Resumen:"
echo ""

psql -d $DB_NAME << 'SQL'
SELECT 
tablename,
pg_size_pretty(pg_total_relation_size('public.'||tablename)) AS tamaño,
n_live_tup as filas
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size('public.'||tablename) DESC;
SQL

echo ""
echo "🎉 Base de datos lista!"
echo ""
echo "Conectar con: psql -d tpch_benchmark"
