# Registro de cambios — Memoria Prácticas (formato/estilo)

Archivo generado: **GabrielCardaba_memoria_FORMATEADA.docx** (copia del documento suspendido; el original no se ha modificado).

## Estilo y formato aplicado
- **Numeración de capítulos unificada y coherente con la normativa** (antes había numeración mixta manual + automática que se rompía):
  - `3.1–3.5` (Introducción, Antecedentes, Estado del arte, Necesidad detectada, Objetivos): pasaban de numeración automática frágil a manual.
  - `Metodología`: era “4.”, ahora **“5. Metodología”**.
  - `6.1` Resumen de contribuciones, `6.3` Recursos empleados, `6.4` Valoración económica renumerados.
  - `Uso de la IA`: era “11.”, ahora **“10. Uso de la IA”**.
- **Glosario**: el antiguo “10. Tecnicismos” se renombró **“Glosario de términos”** y se **movió al inicio** (antes del Resumen), como pide la normativa.
- **Orden del preliminar**: Índice → Glosario de términos → Índice de ilustraciones → Índice de tablas.
- **Índices regenerados** (campos actualizados): el índice general, el de ilustraciones y el de tablas reflejan la nueva estructura.
- **Índice de tablas**: estaba vacío (placeholder). Se creó un campo TOC para tablas; ahora lista la nueva **Tabla 1**.
- **Pies de figura (captions)**: se añadió el campo automático SEQ a 3 figuras que lo habían perdido y se eliminaron 2 pies vacíos; así la numeración de figuras es continua y el índice de ilustraciones se completa.
- **Fuente unificada**: cuerpo en **Arial 11**, justificado, interlineado 1,5 (se mantiene monoespaciada en bloques de código y Cambria Math en fórmulas). Se eliminaron restos de Inter/Times.
- **Nombre corregido**: “Gabriel Cáraba López” → **“Gabriel Cárdaba López”** (portada, autorización y resto del documento).

## Contenido añadido
- **4. Marco teórico**: **borrador redactado** (subapartados 4.1–4.6: modelo relacional/SQL, optimización por costes, estimación de cardinalidad, PostgreSQL vs Apache Spark, LLMs para reescritura y benchmark TPC-H). **Generado a partir del proyecto; revísalo y ajústalo.**
- **6.2. Planificación temporal**: sección redactada con tabla de 6 fases (13 oct 2025 – 20 ene 2026) — **contenido generado a petición tuya**; conviene que lo revises.

## Segunda ronda (estilo y contenido)
- **Jerarquía de estilos coherente** aplicada a todo el documento: Normal 11 pt; **Título (H1) 16 pt**, **Subtítulo (H2) 13 pt**, **Sub-subtítulo (H3) 12 pt**; Arial, negrita, color negro.
- **Bloques de código** (bash de generación TPC-H y todas las consultas SQL): formateados en **monoespaciado (Cascadia Code 9 pt) con sombreado**; eliminadas las marcas markdown/comillas literales (```` ```bash ````, `“““ … ”””`).
- **Erratas corregidas**: “Estes estudio” → “Este estudio”; “query’s … derrollados” → “consultas … desarrolladas”; “conultas” → “consultas”.
- **Frase cortada completada** (párrafo sobre Codex/Spider que terminaba en “…alcanzar una alta tasa de”).
- **Pies de figura**: corregido “QUERY  … SPARK LLM” → “QUERY 2 … SPARK por LLM”.

## Tercera ronda (coherencia de interlineado)
- El texto que se había añadido (Marco teórico y la intro de Planificación temporal) y otros 9 párrafos de cuerpo no llevaban el **interlineado 1,5 + 12 pt de separación antes + justificado** del resto del documento. Se han **igualado todos**, de modo que el cuerpo es ahora homogéneo (mismo estilo que, p. ej., "3.4. Necesidad detectada").

## Cuarta ronda (pies e índice de ilustraciones)
- Cada imagen real tenía ya su pie correcto, pero había **2 pies huérfanos sin imagen** (uno mal rotulado como "QUERY 1"); se han **eliminado**. Quedan **9 figuras** reales (Horario + 5 de Query 1 + 3 de Query 2).
- Todos los pies se reescribieron al formato limpio **"Ilustración N. descripción"** (antes el número iba al final, lo que afeaba el índice) y se normalizaron acentos ("Desviación", "típica").
- El **índice de ilustraciones estaba "congelado"** (texto estático con entradas viejas que ni existían). Se **reconstruyó como campo TOC vivo**; ahora lista correctamente Ilustración 1–9 y se regenera al actualizar campos.

## Quinta ronda (versión final)
- **Mejoras visuales**: se aplicó *keepNext/keepLines* a todos los encabezados (ningún título queda huérfano al pie de página) y un **salto de página antes de cada capítulo/índice principal** (16 en total), de modo que cada capítulo empieza en una página nueva.
- **6.1 y 6.3 redactados**: el contenido en modo listado/conceptual de "6.1. Resumen de contribuciones" y "6.3. Recursos empleados" se ha reescrito en **prosa**, conservando toda la información. La tabla de 6.2 se ha dejado intacta.
- **Bibliografía verificada y depurada**: ordenada alfabéticamente, unificada a estilo **APA**, con sangría francesa. Se eliminaron 2 URLs sueltas que duplicaban el paper de Spark SQL (Armbrust et al., 2015), se convirtió la entrada en formato IEEE (Volcano) a APA, y se **completaron 2 entradas truncadas** (Selinger et al., 1979 → pp. 23–34; Graefe, 1995, Cascades → IEEE Data Eng. Bull. 18(3), 19–29). **Verifica esos dos datos completados.**

## Sexta ronda (limpieza de texto de plantilla)
- **2.2. Objetivos de las prácticas**: eliminadas las frases-guía de la plantilla ("Tanto objetivos…", "Recuerda que puedes utilizar los objetivos…"). La sección conserva su **tabla de objetivos real** (que estaba debajo) y ahora lleva una frase introductoria que da paso a la tabla.
- **10. Uso de la IA**: eliminada la instrucción de plantilla ("Sí/No. Si se contesta sí se recomienda…"). Se conserva tu contenido real (citethisforme, usos de IA en investigación, definiciones y resúmenes).

## Séptima ronda (figuras, tablas y sección 10)
- **Sección 10 (Uso de la IA)** reescrita en **prosa formal**, conservando todos los ejemplos (incluido el de «ghosting» con sus dos redacciones, neutra y diplomática).
- **Tablas numeradas y añadidas al índice de tablas**: Tabla 1 (Objetivos de las prácticas) y Tabla 2 (Planificación temporal).
- **Gráficas que faltaban añadidas** desde la carpeta `visualizations`: Ilustración 9 (Query 2, comparación PostgreSQL vs SPARK) e Ilustración 11 (Query 2, mejora porcentual en SPARK por LLM, recortada de `improvements_summary.png`). El índice de ilustraciones tiene ahora **11 figuras**, simétrico entre Query 1 y Query 2.
- **Saltos de página**: cada capítulo e índice principal empieza en página nueva.

## Octava ronda (humanizar: quitar rayas de IA)
- Eliminadas **todas las rayas largas (—) y medias (–)** del documento, que son el marcador típico de texto generado por IA, conservando intactos los guiones normales (TPC-H, back-end, e-commerce, etc.).
- Las 5 rayas largas se **reformularon** con paréntesis, comas o dos puntos (p. ej. "ANALYZE: collect…", el inciso de `load_final.sh`, el de estimación de cardinalidad).
- Las rayas medias se pasaron a guion normal: nombre de empresa ("VML - The Cocktail"), rangos de páginas de la bibliografía (p. ej. 73-169) y fechas de la tabla de planificación.
- *Nota*: en APA las páginas suelen ir con raya media (73–169); si tu tutor lo prefiere así, se puede revertir solo en la bibliografía. Las muletillas tipo "No solo… sino también" que quedan están en **tu texto original** (Resultados/Conclusiones), no se han tocado.

## Novena ronda (coherencia contenido ↔ afirmaciones)
Revisión de que lo que afirma 6.1 esté realmente respaldado por el documento:
- **Plantillas**: 6.1 decía "plantillas de reescritura específicas por motor que ofrecen mejores resultados que las guías genéricas". En realidad el documento contiene **una** plantilla de prompt sensible al motor (en la metodología) con directrices separadas para PostgreSQL y para Spark, y **no hay** ninguna comparación contra "guías genéricas". Se ha reescrito la afirmación para que case con lo documentado (plantilla única, con remisión a la metodología) y se ha eliminado la comparación no sustentada.
- **Nombre de script**: 6.1 citaba `benchmark_with_visualizations.py`, archivo que **no existe** en el proyecto. El script real de benchmarking y visualización es `querysTPCHAnalysis.py` (≈950 líneas, el que cita la metodología). Corregido el nombre y el nº de líneas.
- Eliminado un **placeholder de plantilla** olvidado dentro de "5. Metodología" ("[En este capítulo se debe indicar la metodología implementada…]").
- El resto de contribuciones (comparación sistemática, metodología reproducible, pipeline TPC-H con `load_final.sh` y `ConvertToParquet.scala`, verificador `verify_semantic_equivalence.py`, 7 archivos de visualización) sí están respaldadas por el contenido o por los ficheros del proyecto.

## Décima ronda (consistencia de estilo final)
- **Negrita**: el párrafo "Fase 2: Dataset TPC-H…" estaba **entero en negrita**; ahora solo la etiqueta "Fase 2: Dataset TPC-H." va en negrita y el resto en redonda (igual que Fase 1 y Fase 3).
- **Interlineado**: unificado a **1,5 en todo el cuerpo**, incluida la bibliografía y unas líneas de resultados que iban a 1,15.
- **Huecos**: eliminados **56 párrafos vacíos** sobrantes que creaban gaps desiguales; el espaciado vertical ahora es uniforme (lo da la separación de párrafo, no líneas en blanco sueltas). Se conservó un espacio antes de cada figura y los saltos de sección.

## Pendiente / a tener en cuenta
1. **Revisar el borrador del Marco teórico (4)** y la **Planificación temporal (6.2)**: son contenido propuesto, no validado por ti.
2. **Verificar 2 datos bibliográficos** completados: Selinger et al. (1979), pp. 23–34; Graefe (1995), IEEE Data Eng. Bull. 18(3), 19–29.
3. Cuestión menor: la subsección “Fase 4: Evaluación y Medición” (dentro de 5. Metodología) va sin numerar.

> Importante: al abrir en Word, acepta **actualizar campos** (o selecciona todo y pulsa F9) para que el índice, el índice de ilustraciones, el de tablas y los números de página se recalculen con la paginación final.

> Nota: al abrir en Word, si pide actualizar campos, acepta (o selecciona todo y pulsa F9) para refrescar índices y numeración de figuras.
