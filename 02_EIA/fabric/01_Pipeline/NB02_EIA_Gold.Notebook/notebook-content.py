# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "696c8c72-1285-4c3f-b87d-a10e76f18ffe",
# META       "default_lakehouse_name": "LH_EIA",
# META       "default_lakehouse_workspace_id": "01623ca8-a4b9-434d-96a5-a96ce81b2f3f"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Capa Gold para Power BI
# Silver conserva cada observación de los 9 datasets. Gold arma un modelo en estrella: dimensiones de fecha, región eléctrica, fuente de generación y serie, más hechos diarios, semanales y mensuales listos para un informe.

# MARKDOWN ********************

# ## Dependencias y propósito

# CELL ********************

# Capa Gold: modelo en estrella listo para Power BI.
# Silver conserva cada observación; Gold reúne métricas de lectura rápida.

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Comprobar las tablas Silver

# CELL ********************

# 1. Leer las 9 tablas Silver y comprobar su clave natural.
contexto = notebookutils.runtime.context
assert contexto.get("defaultLakehouseName") == "LH_EIA", (
    "Vincula LH_EIA como Lakehouse predeterminado."
)
CLAVES_SILVER = {
    "rto_demanda_region": ["region", "tipo", "zona_horaria", "fecha"],
    "rto_generacion_combustible": ["region", "fuente", "zona_horaria", "fecha"],
    "rto_intercambio": ["region_origen", "region_destino", "zona_horaria", "fecha"],
    "precio_minorista_combustibles": ["serie", "fecha"],
    "precio_spot_crudo": ["serie", "fecha"],
    "inventarios_semanales": ["serie", "fecha"],
    "almacenamiento_gas": ["serie", "fecha"],
    "precio_henry_hub": ["serie", "fecha"],
    "consumo_gas_sector": ["serie", "fecha"],
}
silver = {}
for nombre, clave in CLAVES_SILVER.items():
    datos = spark.table(f"silver.{nombre}")
    filas_silver = datos.count()
    assert filas_silver > 0, f"silver.{nombre} está vacía: ejecuta antes NB01_EIA_Silver."
    duplicados_silver = datos.groupBy(*clave).count().where(F.col("count") > 1).count()
    assert duplicados_silver == 0, f"silver.{nombre} tiene claves repetidas."
    silver[nombre] = datos
    print(f"silver.{nombre:32} {filas_silver:>8,} observaciones válidas, {duplicados_silver} duplicados")

# Los hechos eléctricos no llevan zona horaria: todo Silver debe venir en una sola.
for nombre in ("rto_demanda_region", "rto_generacion_combustible", "rto_intercambio"):
    zonas = silver[nombre].select("zona_horaria").distinct().count()
    assert zonas == 1, f"silver.{nombre} mezcla {zonas} zonas horarias."

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Crear las dimensiones de fecha, región, fuente y serie

# CELL ********************

# 2. Dimensiones. Las claves son estables y únicas. Cuando EIA usa más de un
# nombre para el mismo código, se conserva el más frecuente.
def nombre_mas_frecuente(datos, codigo, nombre):
    ventana = Window.partitionBy(codigo).orderBy(F.col("count").desc(), F.col(nombre).asc())
    return (
        datos.groupBy(codigo, nombre).count()
        .withColumn("posicion", F.row_number().over(ventana))
        .where(F.col("posicion") == 1)
        .select(codigo, nombre)
    )


def mapa(diccionario):
    return F.create_map(*[F.lit(x) for par in diccionario.items() for x in par])


# Fecha: un día por fila, desde la primera hasta la última observación de Silver.
limites = reduce(DataFrame.unionByName, [d.select("fecha") for d in silver.values()]) \
    .agg(F.min("fecha").alias("inicio"), F.max("fecha").alias("fin")).first()
MESES = {1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio", 7: "julio",
         8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"}
DIAS = {1: "lunes", 2: "martes", 3: "miércoles", 4: "jueves", 5: "viernes", 6: "sábado", 7: "domingo"}
dia_semana = (F.dayofweek("fecha") + 5) % 7 + 1   # 1 = lunes … 7 = domingo
dim_fecha = spark.sql(
    f"SELECT explode(sequence(DATE'{limites['inicio']}', DATE'{limites['fin']}', INTERVAL 1 DAY)) AS fecha"
).select(
    "fecha",
    F.year("fecha").alias("anio"),
    F.quarter("fecha").alias("trimestre"),
    F.month("fecha").alias("mes"),
    mapa(MESES)[F.month("fecha")].alias("nombre_mes"),
    F.date_format("fecha", "yyyy-MM").alias("etiqueta_mes"),
    F.weekofyear("fecha").alias("semana_iso"),
    dia_semana.alias("dia_semana"),
    mapa(DIAS)[dia_semana].alias("nombre_dia"),
    (dia_semana >= 6).alias("es_fin_de_semana"),
)

# Región eléctrica: las 8 regiones analizadas y sus vecinas en el intercambio.
REGIONES_ANALIZADAS = ["US48", "CISO", "ERCO", "PJM", "MISO", "NYIS", "ISNE", "SWPP"]
intercambio = silver["rto_intercambio"]
regiones = reduce(DataFrame.unionByName, [
    silver["rto_demanda_region"].select("region", "nombre_region"),
    silver["rto_generacion_combustible"].select("region", "nombre_region"),
    intercambio.select(F.col("region_origen").alias("region"), F.col("nombre_region_origen").alias("nombre_region")),
    intercambio.select(F.col("region_destino").alias("region"), F.col("nombre_region_destino").alias("nombre_region")),
])
dim_region_electrica = nombre_mas_frecuente(regiones, "region", "nombre_region").select(
    "region",
    "nombre_region",
    (F.col("region") == "US48").alias("es_total_nacional"),
    F.col("region").isin(REGIONES_ANALIZADAS).alias("es_region_analizada"),
)

# Fuente de generación: nombre en español y grupo para el análisis de la matriz.
FUENTES = {
    "COL": ("Carbón", "Fósil"), "NG": ("Gas natural", "Fósil"), "OIL": ("Petróleo", "Fósil"),
    "NUC": ("Nuclear", "Nuclear"),
    "SUN": ("Solar", "Renovable"), "WND": ("Eólica", "Renovable"), "WAT": ("Hidroeléctrica", "Renovable"),
    "GEO": ("Geotérmica", "Renovable"), "SNB": ("Solar con baterías", "Renovable"),
    "WNB": ("Eólica con baterías", "Renovable"),
    "BAT": ("Baterías", "Almacenamiento"), "PS": ("Bombeo hidráulico", "Almacenamiento"),
    "OES": ("Otro almacenamiento", "Almacenamiento"), "UES": ("Almacenamiento no identificado", "Almacenamiento"),
    "OTH": ("Otras", "Otras"), "UNK": ("Desconocida", "Otras"),
}
fuentes_es = spark.createDataFrame(
    [(codigo, nombre, grupo) for codigo, (nombre, grupo) in FUENTES.items()],
    "fuente string, nombre_fuente string, grupo_fuente string",
)
dim_fuente_generacion = (
    nombre_mas_frecuente(silver["rto_generacion_combustible"], "fuente", "nombre_fuente")
    .withColumnRenamed("nombre_fuente", "nombre_fuente_eia")
    .join(fuentes_es, "fuente", "left")
    .select(
        "fuente",
        F.coalesce("nombre_fuente", "nombre_fuente_eia").alias("nombre_fuente"),
        "nombre_fuente_eia",
        F.coalesce("grupo_fuente", F.lit("Otras")).alias("grupo_fuente"),
        (F.col("grupo_fuente") == "Renovable").alias("es_renovable"),
    )
)

# Serie: petróleo y gas natural. Los atributos se toman de la observación más reciente.
DATASETS_SERIE = {
    "precio_minorista_combustibles": ("petroleum", "Precio minorista", "Semanal"),
    "precio_spot_crudo": ("petroleum", "Precio spot", "Diaria"),
    "inventarios_semanales": ("petroleum", "Inventario", "Semanal"),
    "almacenamiento_gas": ("natural-gas", "Almacenamiento", "Semanal"),
    "precio_henry_hub": ("natural-gas", "Precio spot", "Diaria"),
    "consumo_gas_sector": ("natural-gas", "Consumo", "Mensual"),
}
ATRIBUTOS_SERIE = ["serie", "descripcion_serie", "area", "nombre_area", "producto",
                   "nombre_producto", "proceso", "nombre_proceso", "unidad"]
ventana_reciente = Window.partitionBy("serie").orderBy(F.col("fecha").desc())
dim_serie = reduce(DataFrame.unionByName, [
    silver[nombre].withColumn("posicion", F.row_number().over(ventana_reciente))
    .where(F.col("posicion") == 1)
    .select(*ATRIBUTOS_SERIE,
            F.lit(nombre).alias("dataset"),
            F.lit(categoria).alias("categoria"),
            F.lit(medida).alias("tipo_medida"),
            F.lit(frecuencia).alias("frecuencia"))
    for nombre, (categoria, medida, frecuencia) in DATASETS_SERIE.items()
])
assert dim_serie.groupBy("serie").count().where(F.col("count") > 1).count() == 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Calcular los hechos de electricidad

# CELL ********************

# 3. Hechos diarios de la red eléctrica. Todos los valores están en MWh.
# Una fila por región y día con demanda, pronóstico, generación e intercambio.
TIPOS = {"D": "demanda_mwh", "DF": "pronostico_demanda_mwh",
         "NG": "generacion_neta_mwh", "TI": "intercambio_total_mwh"}
fact_electricidad_diaria = (
    silver["rto_demanda_region"]
    .groupBy("fecha", "region").pivot("tipo", list(TIPOS)).agg(F.first("valor"))
)
for codigo, columna in TIPOS.items():
    fact_electricidad_diaria = fact_electricidad_diaria.withColumnRenamed(codigo, columna)
fact_electricidad_diaria = fact_electricidad_diaria.withColumn(
    "error_pronostico_pct",
    F.when(F.col("demanda_mwh") > 0,
           F.round(100 * (F.col("pronostico_demanda_mwh") / F.col("demanda_mwh") - 1), 2)),
)

# Una fila por región, día y fuente. La participación se calcula sobre la
# generación positiva: el almacenamiento resta energía cuando carga.
generacion_positiva = F.greatest(F.col("generacion_mwh"), F.lit(0.0))
total_dia = F.sum(generacion_positiva).over(Window.partitionBy("fecha", "region"))
fact_generacion_diaria = (
    silver["rto_generacion_combustible"]
    .select("fecha", "region", "fuente", F.col("valor").alias("generacion_mwh"))
    .withColumn("participacion_pct",
                F.when(total_dia > 0, F.round(100 * generacion_positiva / total_dia, 2)))
)

# Una fila por par de regiones y día. Positivo: la región origen envía energía;
# negativo: la recibe.
fact_intercambio_diario = silver["rto_intercambio"].select(
    "fecha", "region_origen", "region_destino", F.col("valor").alias("intercambio_mwh")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Calcular los hechos de petróleo y gas natural

# CELL ********************

# 4. Hechos por serie, separados por granularidad para no mezclar días, semanas
# y meses en una misma tabla. La unidad de cada serie está en dim_serie.
def hechos_serie(nombres, columna_valor):
    return reduce(DataFrame.unionByName, [
        silver[n].select("fecha", "serie", F.col("valor").alias(columna_valor)) for n in nombres
    ])


fact_precios_diarios = hechos_serie(["precio_spot_crudo", "precio_henry_hub"], "precio")
fact_precio_combustible_semanal = hechos_serie(["precio_minorista_combustibles"], "precio")
fact_inventarios_semanales = hechos_serie(["inventarios_semanales", "almacenamiento_gas"], "valor")
fact_consumo_gas_mensual = hechos_serie(["consumo_gas_sector"], "consumo_mmcf")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Calcular el hecho mensual de precios

# CELL ********************

# 5. Hecho mensual de precios. Una fila por serie y mes; el promedio no sustituye
# las observaciones diarias y semanales originales, que siguen en Silver.
precios = hechos_serie(
    ["precio_minorista_combustibles", "precio_spot_crudo", "precio_henry_hub"], "precio"
)
fact_precios_mensuales = precios.withColumn("fecha_mes", F.trunc(F.col("fecha"), "MM")) \
    .groupBy("serie", "fecha_mes") \
    .agg(
        F.round(F.avg("precio"), 3).alias("precio_promedio"),
        F.min("precio").alias("precio_minimo"),
        F.max("precio").alias("precio_maximo"),
        F.count("*").cast("int").alias("observaciones"),
    )
ventana_mes = Window.partitionBy("serie").orderBy("fecha_mes")
fact_precios_mensuales = fact_precios_mensuales.withColumn(
    "precio_mes_anterior", F.lag("precio_promedio").over(ventana_mes),
).withColumn(
    "variacion_mensual_pct",
    F.when(F.col("precio_mes_anterior") > 0,
           F.round(100 * (F.col("precio_promedio") / F.col("precio_mes_anterior") - 1), 2)),
).select(
    "serie", "fecha_mes", "precio_promedio", "precio_minimo", "precio_maximo",
    "observaciones", "precio_mes_anterior", "variacion_mensual_pct",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Calcular el último precio de cada serie

# CELL ********************

# 6. Hecho de último precio para las tarjetas KPI del informe. dias_desde_ultimo_dato
# avisa si una serie dejó de publicarse.
ventana_ultima = Window.partitionBy("serie").orderBy(F.col("fecha").desc())
fact_ultimo_precio = precios.withColumn("posicion", F.row_number().over(ventana_ultima)) \
    .withColumn("precio_anterior", F.lead("precio").over(ventana_ultima)) \
    .where(F.col("posicion") == 1) \
    .withColumn(
        "variacion_pct",
        F.when(F.col("precio_anterior") > 0,
               F.round(100 * (F.col("precio") / F.col("precio_anterior") - 1), 2)),
    ).withColumn(
        "dias_desde_ultimo_dato", F.datediff(F.current_date(), F.col("fecha"))
    ).select(
        "serie", "fecha", "precio", "precio_anterior", "variacion_pct", "dias_desde_ultimo_dato",
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Materializar las tablas Gold

# CELL ********************

# 7. Materializar Gold. Es una capa derivada: se reconstruye desde Silver en
# cada ejecución para incorporar las nuevas observaciones.
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
salidas = {
    "gold.dim_fecha": dim_fecha,
    "gold.dim_region_electrica": dim_region_electrica,
    "gold.dim_fuente_generacion": dim_fuente_generacion,
    "gold.dim_serie": dim_serie,
    "gold.fact_electricidad_diaria": fact_electricidad_diaria,
    "gold.fact_generacion_diaria": fact_generacion_diaria,
    "gold.fact_intercambio_diario": fact_intercambio_diario,
    "gold.fact_precios_diarios": fact_precios_diarios,
    "gold.fact_precio_combustible_semanal": fact_precio_combustible_semanal,
    "gold.fact_inventarios_semanales": fact_inventarios_semanales,
    "gold.fact_consumo_gas_mensual": fact_consumo_gas_mensual,
    "gold.fact_precios_mensuales": fact_precios_mensuales,
    "gold.fact_ultimo_precio": fact_ultimo_precio,
}
for nombre, datos in salidas.items():
    datos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(nombre)
    print("Escrita", nombre)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Validar recuentos, claves y métricas

# CELL ********************

# 8. Validaciones para explicar Gold con cifras, no solo con un diagrama.
def sin_duplicados(tabla, clave):
    repetidas = spark.table(tabla).groupBy(*clave).count().where(F.col("count") > 1).count()
    assert repetidas == 0, f"{tabla} tiene {repetidas} claves repetidas"


def sin_huerfanos(tabla_hecho, columna, tabla_dim, columna_dim=None):
    dim = spark.table(tabla_dim).select(F.col(columna_dim or columna).alias(columna))
    huerfanos = spark.table(tabla_hecho).select(columna).distinct().join(dim, columna, "left_anti").count()
    assert huerfanos == 0, f"{tabla_hecho}.{columna} tiene {huerfanos} valores sin {tabla_dim}"


conteos = {nombre: spark.table(nombre).count() for nombre in salidas}
assert all(filas > 0 for filas in conteos.values()), f"Hay tablas Gold vacías: {conteos}"

sin_duplicados("gold.dim_fecha", ["fecha"])
sin_duplicados("gold.dim_region_electrica", ["region"])
sin_duplicados("gold.dim_fuente_generacion", ["fuente"])
sin_duplicados("gold.dim_serie", ["serie"])
sin_duplicados("gold.fact_electricidad_diaria", ["fecha", "region"])
sin_duplicados("gold.fact_generacion_diaria", ["fecha", "region", "fuente"])
sin_duplicados("gold.fact_intercambio_diario", ["fecha", "region_origen", "region_destino"])
sin_duplicados("gold.fact_precios_mensuales", ["serie", "fecha_mes"])

for hecho in ("fact_precios_diarios", "fact_precio_combustible_semanal",
              "fact_inventarios_semanales", "fact_consumo_gas_mensual",
              "fact_precios_mensuales", "fact_ultimo_precio"):
    sin_huerfanos(f"gold.{hecho}", "serie", "gold.dim_serie")
sin_huerfanos("gold.fact_electricidad_diaria", "region", "gold.dim_region_electrica")
sin_huerfanos("gold.fact_generacion_diaria", "region", "gold.dim_region_electrica")
sin_huerfanos("gold.fact_generacion_diaria", "fuente", "gold.dim_fuente_generacion")
sin_huerfanos("gold.fact_intercambio_diario", "region_origen", "gold.dim_region_electrica", "region")
sin_huerfanos("gold.fact_intercambio_diario", "region_destino", "gold.dim_region_electrica", "region")

assert spark.table("gold.fact_precios_mensuales").agg(F.sum("observaciones")).first()[0] == precios.count()
assert conteos["gold.fact_ultimo_precio"] == precios.select("serie").distinct().count()
assert conteos["gold.fact_electricidad_diaria"] == \
    silver["rto_demanda_region"].select("fecha", "region").distinct().count()

print("Gold:")
for nombre, filas in conteos.items():
    print(f"  {nombre:40} {filas:>8,} filas")

print("Último precio por serie (petróleo y gas):")
spark.table("gold.fact_ultimo_precio").join(spark.table("gold.dim_serie"), "serie") \
    .where(F.col("area").isin("NUS", "YCUOK", "ZEU", "RGC")) \
    .select("descripcion_serie", "fecha", "precio", "unidad", "variacion_pct", "dias_desde_ultimo_dato") \
    .orderBy("descripcion_serie").show(truncate=False)

print("Última semana de la red eléctrica nacional (US48):")
spark.table("gold.fact_electricidad_diaria").where(F.col("region") == "US48") \
    .orderBy(F.col("fecha").desc()).show(7, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
