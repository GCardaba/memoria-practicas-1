#!/bin/zsh

echo "🔍 Diagnóstico de archivos .tbl"
echo "==============================="
echo ""

# 1. Ver formato de region.tbl
echo "📄 Primeras 3 líneas de region.tbl:"
head -3 region.tbl
echo ""

# 2. Contar pipes por línea
echo "🔢 Número de delimitadores '|' por línea (primera línea):"
head -1 region.tbl | tr -cd '|' | wc -c
echo ""

# 3. Ver schema esperado
echo "📋 Schema de region en PostgreSQL:"
psql -d tpch_benchmark -c "\d region"
echo ""

# 4. Test de carga con más detalle
echo "🧪 Intentando cargar region.tbl..."
psql -d tpch_benchmark << 'SQL'
-- Limpiar tabla
TRUNCATE region CASCADE;

-- Intentar cargar
\COPY region FROM 'region.tbl' DELIMITER '|' CSV;

-- Ver resultado
SELECT COUNT(*) as filas_cargadas FROM region;
SELECT * FROM region LIMIT 3;
SQL

echo ""
echo "🔍 Si el count es 0, hay problema de formato"
echo "🔍 Si da error, lo veremos arriba"
