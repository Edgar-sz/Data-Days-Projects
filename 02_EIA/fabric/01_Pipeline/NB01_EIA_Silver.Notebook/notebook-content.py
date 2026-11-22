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

# # Histórico acumulativo de datos de energía (EIA)
# El pipeline `PL_EIA` copia a Bronze la respuesta de la API de EIA para 9 datasets de electricidad, petróleo y gas natural. Este cuaderno lee todo Bronze, valida, deduplica e inserta únicamente las observaciones nuevas en una tabla Silver por dataset.

# MARKDOWN ********************

# ## Importaciones y dependencias

# CELL ********************

# Histórico acumulativo de los 9 datasets de EIA.
# Fuente: EIA Open Data API v2, copiada previamente por el pipeline PL_EIA a Files/bronze/eia.

from pathlib import Path
import json

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Definir los datasets y su clave natural
# Cada dataset se guarda en su propia tabla Silver. La clave natural identifica una observación: si ya existe en Silver, no se vuelve a insertar y se conserva el primer valor incorporado.
# 
# `dias_cierre` evita guardar días incompletos: EIA publica la electricidad del día en curso mientras todavía llegan reportes, y como Silver conserva el primer valor recibido, solo se aceptan días con al menos 2 días de antigüedad.

# CELL ********************

# 1. Qué columnas se conservan de cada dataset, cuál es su clave natural y qué
# valores se consideran válidos. Los nombres de la API se pasan a snake_case.
COLUMNAS_SERIE = {
    "series": "serie",
    "series-description": "descripcion_serie",
    "duoarea": "area",
    "area-name": "nombre_area",
    "product": "producto",
    "product-name": "nombre_producto",
    "process": "proceso",
    "process-name": "nombre_proceso",
}
CLAVE_SERIE = ["serie", "fecha"]

DATASETS = {
    "rto_demanda_region": {
        "categoria": "electricity",
        "formato_fecha": "yyyy-MM-dd",
        "columna_unidad": "value-units",
        "columnas": {"respondent": "region", "respondent-name": "nombre_region",
                     "type": "tipo", "type-name": "nombre_tipo", "timezone": "zona_horaria"},
        "clave": ["region", "tipo", "zona_horaria", "fecha"],
        "permite_negativos": True,   # el intercambio total es negativo cuando la región importa
        "dias_cierre": 2,
    },
    "rto_generacion_combustible": {
        "categoria": "electricity",
        "formato_fecha": "yyyy-MM-dd",
        "columna_unidad": "value-units",
        "columnas": {"respondent": "region", "respondent-name": "nombre_region",
                     "fueltype": "fuente", "type-name": "nombre_fuente", "timezone": "zona_horaria"},
        "clave": ["region", "fuente", "zona_horaria", "fecha"],
        "permite_negativos": True,   # baterías y bombeo consumen energía al cargar
        "dias_cierre": 2,
    },
    "rto_intercambio": {
        "categoria": "electricity",
        "formato_fecha": "yyyy-MM-dd",
        "columna_unidad": "value-units",
        "columnas": {"fromba": "region_origen", "fromba-name": "nombre_region_origen",
                     "toba": "region_destino", "toba-name": "nombre_region_destino",
                     "timezone": "zona_horaria"},
        "clave": ["region_origen", "region_destino", "zona_horaria", "fecha"],
        "permite_negativos": True,   # el signo indica el sentido del flujo
        "dias_cierre": 2,
    },
    "precio_minorista_combustibles": {
        "categoria": "petroleum", "formato_fecha": "yyyy-MM-dd", "columna_unidad": "units",
        "columnas": COLUMNAS_SERIE, "clave": CLAVE_SERIE,
        "permite_negativos": False, "dias_cierre": 0,
    },
    "precio_spot_crudo": {
        "categoria": "petroleum", "formato_fecha": "yyyy-MM-dd", "columna_unidad": "units",
        "columnas": COLUMNAS_SERIE, "clave": CLAVE_SERIE,
        "permite_negativos": False, "dias_cierre": 0,
    },
    "inventarios_semanales": {
        "categoria": "petroleum", "formato_fecha": "yyyy-MM-dd", "columna_unidad": "units",
        "columnas": COLUMNAS_SERIE, "clave": CLAVE_SERIE,
        "permite_negativos": False, "dias_cierre": 0,
    },
    "almacenamiento_gas": {
        "categoria": "natural-gas", "formato_fecha": "yyyy-MM-dd", "columna_unidad": "units",
        "columnas": COLUMNAS_SERIE, "clave": CLAVE_SERIE,
        "permite_negativos": False, "dias_cierre": 0,
    },
    "precio_henry_hub": {
        "categoria": "natural-gas", "formato_fecha": "yyyy-MM-dd", "columna_unidad": "units",
        "columnas": COLUMNAS_SERIE, "clave": CLAVE_SERIE,
        "permite_negativos": False, "dias_cierre": 0,
    },
    "consumo_gas_sector": {
        "categoria": "natural-gas", "formato_fecha": "yyyy-MM", "columna_unidad": "units",
        "columnas": COLUMNAS_SERIE, "clave": CLAVE_SERIE,
        "permite_negativos": False, "dias_cierre": 0,
    },
}
print(f"Datasets configurados: {len(DATASETS)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Comprobar el Lakehouse y los archivos de entrada

# CELL ********************

# 2. Comprobar contexto y archivos Bronze. Cada ejecución del pipeline deja un
# archivo nuevo por dataset. Volver a leerlos todos hace la carga reproducible;
# MERGE impide que vuelvan a entrar las claves ya presentes en Silver.
contexto = notebookutils.runtime.context
assert contexto.get("defaultLakehouseName") == "LH_EIA", (
    "Vincula LH_EIA como Lakehouse predeterminado."
)
carpeta_bronze = Path("/lakehouse/default/Files/bronze/eia")
archivos = {}
for nombre, cfg in DATASETS.items():
    encontrados = sorted((carpeta_bronze / cfg["categoria"] / nombre).glob("*.json"))
    if not encontrados:
        raise FileNotFoundError(
            f"No hay JSON de {nombre} en Files/bronze/eia/{cfg['categoria']}/{nombre}. "
            "Ejecuta antes el pipeline PL_EIA."
        )
    archivos[nombre] = encontrados
    print(f"{nombre:32} {len(encontrados):>4} archivos Bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Leer la respuesta EIA y validar observaciones
# Cada archivo Bronze tiene una línea por página de la API (JSON Lines) con las observaciones en `response.data`. Spark lee todas las páginas de todos los archivos del dataset y separa cada observación.
# 
# Una observación es válida si tiene fecha, valor numérico y clave completa; en precios, inventarios y consumo el valor además debe ser mayor que cero.

# CELL ********************

# 3. Lector de Bronze. Todas las columnas de la API se leen como texto y luego
# se convierten: period a fecha y value a número. La fecha de modificación del
# archivo queda como momento de captura, igual que en la versión original.
def esquema_bronze(columnas_api):
    observacion = T.StructType([T.StructField(c, T.StringType()) for c in columnas_api])
    return T.StructType([
        T.StructField("response", T.StructType([
            T.StructField("data", T.ArrayType(observacion))
        ]))
    ])


def leer_bronze(nombre, cfg):
    columnas_api = ["period", "value", cfg["columna_unidad"], *cfg["columnas"]]
    ruta = f"Files/bronze/eia/{cfg['categoria']}/{nombre}/*.json"
    observaciones = (
        spark.read.schema(esquema_bronze(columnas_api)).json(ruta)
        .select(
            F.explode("response.data").alias("r"),
            F.col("_metadata.file_modification_time").alias("capturado_en_utc"),
            F.col("_metadata.file_name").alias("archivo_origen"),
        )
        .select(
            *[F.col(f"r.`{api}`").alias(silver) for api, silver in cfg["columnas"].items()],
            F.to_date(F.col("r.period"), cfg["formato_fecha"]).alias("fecha"),
            F.col("r.value").cast("double").alias("valor"),
            F.col(f"r.`{cfg['columna_unidad']}`").alias("unidad"),
            "capturado_en_utc",
            "archivo_origen",
        )
    )

    valida = F.col("fecha").isNotNull() & F.col("valor").isNotNull()
    for columna in cfg["clave"]:
        valida = valida & F.col(columna).isNotNull()
    if not cfg["permite_negativos"]:
        valida = valida & (F.col("valor") > 0)
    cerrada = F.col("fecha") <= F.date_sub(F.current_date(), cfg["dias_cierre"])

    observaciones = observaciones.withColumn("es_valida", valida).withColumn("esta_cerrada", cerrada)
    validas = (
        observaciones.where("es_valida AND esta_cerrada")
        .drop("es_valida", "esta_cerrada")
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
    )
    conteo = observaciones.agg(
        F.count("*").alias("filas_leidas"),
        F.sum(F.when(~F.col("es_valida"), 1).otherwise(0)).alias("filas_invalidas"),
        F.sum(F.when(F.col("es_valida") & ~F.col("esta_cerrada"), 1).otherwise(0)).alias("filas_dia_sin_cerrar"),
    ).first().asDict()
    return validas, conteo

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Quitar duplicados de la entrada por clave natural

# CELL ********************

# 4. Deduplicar dentro de la entrada. Como cada archivo Bronze repite la historia
# desde la fecha de inicio, una misma clave aparece en muchos archivos. Si un
# archivo posterior trae una corrección, este histórico append conserva el primer
# valor incorporado; las correcciones requieren una política aparte.
def quitar_duplicados(validas, clave):
    ventana = Window.partitionBy(*clave).orderBy(
        F.col("capturado_en_utc").asc(), F.col("archivo_origen").asc()
    )
    return (
        validas.withColumn("orden", F.row_number().over(ventana))
        .where(F.col("orden") == 1)
        .drop("orden")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Insertar solo claves nuevas en Silver particionada

# CELL ********************

# 5. Crear cada tabla Silver Delta una sola vez, particionada por año y mes, y
# agregar exclusivamente las claves todavía ausentes. Repetir la carga es idempotente.
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
resumen = {}
for nombre, cfg in DATASETS.items():
    tabla = f"silver.{nombre}"
    validas, conteo = leer_bronze(nombre, cfg)
    unicas = quitar_duplicados(validas, cfg["clave"])

    if not spark.catalog.tableExists(tabla):
        unicas.limit(0).write.format("delta").partitionBy("anio", "mes").saveAsTable(tabla)

    antes = spark.table(tabla).count()
    condicion = " AND ".join(f"destino.{c} = origen.{c}" for c in cfg["clave"])
    DeltaTable.forName(spark, tabla).alias("destino").merge(
        unicas.alias("origen"), condicion
    ).whenNotMatchedInsertAll().execute()
    despues = spark.table(tabla).count()

    resumen[nombre] = {
        "archivos_bronze": len(archivos[nombre]),
        **conteo,
        "claves_unicas_en_bronze": unicas.count(),
        "filas_silver_antes": antes,
        "filas_nuevas": despues - antes,
        "filas_silver_despues": despues,
    }
    print(f"{tabla:45} +{despues - antes:>8,} filas nuevas  (total {despues:,})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Comprobar idempotencia, duplicados y particiones

# CELL ********************

# 6. Controles sobre cada tabla Silver: sin claves repetidas y con las
# particiones esperadas. Al final, un resumen por dataset.
for nombre, cfg in DATASETS.items():
    tabla = f"silver.{nombre}"
    duplicados_silver = spark.table(tabla).groupBy(*cfg["clave"]).count() \
        .where(F.col("count") > 1).count()
    if duplicados_silver:
        raise AssertionError(f"{tabla} tiene {duplicados_silver} claves duplicadas")
    particiones = spark.sql(f"DESCRIBE DETAIL {tabla}").select("partitionColumns").first()[0]
    assert particiones == ["anio", "mes"], f"Particiones inesperadas en {tabla}: {particiones}"
    resumen[nombre]["duplicados_en_silver"] = duplicados_silver

print(json.dumps(resumen, ensure_ascii=False, indent=2))

filas = []
for nombre in DATASETS:
    fila = spark.table(f"silver.{nombre}").agg(
        F.count("*").alias("observaciones"),
        F.min("fecha").alias("primera_fecha"),
        F.max("fecha").alias("ultima_fecha"),
    ).first()
    filas.append((nombre, fila["observaciones"], fila["primera_fecha"], fila["ultima_fecha"]))
spark.createDataFrame(
    filas, "tabla string, observaciones long, primera_fecha date, ultima_fecha date"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
