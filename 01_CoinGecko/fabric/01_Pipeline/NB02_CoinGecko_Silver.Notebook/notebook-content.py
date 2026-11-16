# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "17c320fa-be26-4398-af85-1a89d46bc981",
# META       "default_lakehouse_name": "LH01_ObservatorioCripto",
# META       "default_lakehouse_workspace_id": "df25e906-6a97-41a2-9eed-5e50802222d0",
# META       "known_lakehouses": [
# META         {
# META           "id": "17c320fa-be26-4398-af85-1a89d46bc981"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver · Calidad y tipado desde bronze.mercado_raw
# 
# **Objetivo de esta capa: Silver.** Caso docente CoinGecko, muestra de hasta
# 100 activos en USD; no representa todo el mercado ni una cartera de inversión.
# 
# **Antes de ejecutar:** este notebook debe tener asociado como Lakehouse predeterminado
# `LH01_ObservatorioCripto`. `spark` lo proporciona Fabric; `notebookutils` permite trabajar
# con sus archivos y la identidad de quien ejecuta. Las rutas `Files/...` son relativas a
# ese Lakehouse. Las tablas `bronze.*`, `silver.*` y `gold.*` están en **Tables**, separadas
# del árbol de archivos. En **Files** solo usamos la carpeta raíz **bronze**:
# 
# - `bronze/coingecko/fecha=.../batch_id=.../`: respuesta original, manifiesto y resultado del lote.
# - `bronze/_control/coingecko/`: lote actual y autorización de Silver para publicar.
# - `bronze/_publicacion/`: exportaciones de evidencia y resultado del refresco del modelo.
# - `bronze/_pruebas/`: copias identificadas para los ensayos de calidad.
# 
# **Orden obligatorio:** Bronze → Silver → Gold. No ejecutar dos cargas simultáneas:
# los punteros de control y las tablas actuales son compartidos. Una celda que falla
# detiene el notebook; no saltarla para forzar una publicación. Todos los tiempos se
# interpretan en UTC. Ejecutar las celdas en orden o usar «Ejecutar todo».
# 
# Cada bloque siguiente explica su entrada, su decisión y su salida. Los `print` finales
# son registros de diagnóstico; no sustituyen comprobar el estado del notebook en Monitor.
# 
# 
# ## Réplica en tu propio entorno
# 
# 1. Crea un espacio de trabajo con capacidad de Fabric (sirve la prueba gratuita) y un Lakehouse llamado `LH01_ObservatorioCripto` con **esquemas habilitados**.
# 2. Importa los tres notebooks y asocia tu Lakehouse como predeterminado en cada uno. La raíz de **Files** solo utiliza `bronze`.
# 3. Ejecuta en orden Bronze, Silver y Gold. El último paso de Gold refresca el modelo semántico `PBI_CoinGecko` del mismo espacio de trabajo, y falla si todavía no existe; las tablas ya estarán escritas.
# 4. Abre el archivo `.pbix` del repositorio, cambia el origen de datos al punto de conexión SQL de tu Lakehouse y publícalo con el nombre `PBI_CoinGecko`. El modelo usa el modo Importar.
# 5. Pipeline (opcional): sustituye la conexión de Outlook y el correo de destino por los tuyos. Cada conexión OAuth se crea con tu propia cuenta.
# 
# Esta edición pública no retira ni borra tablas antiguas.
# 


# MARKDOWN ********************

# ## Parámetros de ejecución
# 
# Esta capa no recibe un lote escrito a mano. Lo recupera de los archivos de control que dejó la etapa anterior dentro de `Files/bronze`. Ejecutar siempre Bronze → Silver → Gold.

# PARAMETERS CELL ********************

# Los parámetros del lote se leen del manifiesto Bronze.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Sesión y herramientas de transformación
# 
# `F` proporciona expresiones de columnas; `T` describe el esquema del JSON. Spark evalúa las transformaciones cuando se pide un resultado o una escritura. Se fija UTC para interpretar de forma coherente las fechas de la fuente.

# CELL ********************

import json
from pyspark.sql import functions as F, types as T
import notebookutils
from pyspark.sql.window import Window

# Las conversiones de timestamp se interpretan en UTC durante toda la sesión.
spark.conf.set("spark.sql.session.timeZone", "UTC")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Identificar y comprobar el lote Bronze
# 
# Se lee `current_batch.json` y se filtra Bronze por ese `batch_id`: no se mezclan extracciones. `cache()` reutiliza ese subconjunto durante los controles. Se exige el conteo del manifiesto, al menos una fila y posiciones `registro_num` únicas. Estos controles detectan inconsistencias de ingesta antes de transformar campos de mercado.

# CELL ********************

manifest = json.loads(notebookutils.fs.head("Files/bronze/_control/coingecko/current_batch.json",1048576))
batch_id, base = manifest["batch_id"], manifest["raw_path"].rsplit("/",1)[0]
received_expected = int(manifest["rows"])
bronze = spark.table("bronze.mercado_raw").filter(F.col("batch_id")==batch_id).cache()
received = bronze.count()
if received != received_expected or received == 0:
    raise ValueError(f"Bronze inconsistente: {received} de {received_expected} filas")
if bronze.select("registro_num").distinct().count() != received:
    raise ValueError("Bronze contiene posiciones duplicadas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Interpretar JSON y tipar sus campos
# 
# `StructType` selecciona los atributos necesarios. Precios, capitalización, volumen y variación se leen como `DoubleType`; el ranking es entero. `from_json` interpreta `record_json` y `record.*` despliega sus campos. Un dato incompatible puede quedar nulo y será evaluado en calidad. Los demás atributos del origen siguen conservados en Bronze aunque no se proyecten en Silver.

# CELL ********************

# Contrato explícito: solo estos atributos se proyectan en Silver; el original sigue en Bronze.
source_schema = T.StructType([
    T.StructField("id",T.StringType()), T.StructField("symbol",T.StringType()), T.StructField("name",T.StringType()),
    T.StructField("current_price",T.DoubleType()), T.StructField("market_cap",T.DoubleType()),
    T.StructField("market_cap_rank",T.LongType()), T.StructField("total_volume",T.DoubleType()),
    T.StructField("price_change_percentage_24h",T.DoubleType()), T.StructField("last_updated",T.StringType())])
raw = bronze.select(F.from_json("record_json",source_schema).alias("record")).select("record.*")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Duplicados y reglas críticas
# 
# La clave es `id`, no el símbolo: dos activos pueden compartir símbolo. La agrupación identifica claves repetidas y el join marca todas sus apariciones. `last_updated` se convierte a timestamp. La expresión `bad` marca identificadores vacíos, nombre nulo, duplicados, importes nulos, NaN o negativos y fechas no interpretables. Se admite precio cero según el contrato. Una variación 24h ausente no es rechazo crítico: se advertirá en Gold sin inventar un cero.

# CELL ********************

# Marcar TODAS las apariciones de una clave repetida; no elegir una al azar.
duplicate_ids = raw.groupBy("id").count().filter("count > 1").select("id").withColumn("duplicate",F.lit(True))
checked = raw.join(duplicate_ids,"id","left").withColumn("fecha_fuente",F.to_timestamp("last_updated"))
# Una condición verdadera basta para rechazar; los paréntesis combinan columnas Spark, no booleanos Python.
bad = (F.col("id").isNull() | (F.trim(F.col("id"))=="") | F.col("name").isNull() |
       F.coalesce(F.col("duplicate"),F.lit(False)) | F.col("current_price").isNull() |
       F.isnan("current_price") | (F.col("current_price")<0) | F.col("market_cap").isNull() |
       F.isnan("market_cap") | (F.col("market_cap")<0) | F.col("total_volume").isNull() |
       F.isnan("total_volume") | (F.col("total_volume")<0) | F.col("fecha_fuente").isNull())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Separar aceptadas y rechazadas
# 
# `filter(bad)` recoge las inválidas y añade lote y motivo; `filter(~bad)` recoge las válidas. Los conteos deben conciliar: recibidas = aceptadas + rechazadas. El motivo actual agrupa varias reglas, no detalla cada campo defectuoso. La separación sirve para explicar por qué un dato no puede llegar al informe.

# CELL ********************

# Guardar el lote y un motivo genérico para poder diagnosticar el rechazo.
rejected_df = checked.filter(bad).withColumn("batch_id",F.lit(batch_id)).withColumn("motivo",F.lit("Clave, tipo, fecha o rango crítico inválido"))
rejected = rejected_df.count()
accepted_df = checked.filter(~bad)
accepted = accepted_df.count()
if received != accepted+rejected:
    raise ValueError("La conciliación Bronze/Silver no coincide")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Persistir rechazos y bloquear el lote inválido
# 
# `silver.rechazos_actuales` se reemplaza con el diagnóstico de este intento, incluso si está vacío. Si existe una sola fila inválida, o ninguna aceptada, se escribe `failure.json` junto al original y se lanza un error. Hasta este punto NO se ha sobrescrito `silver.mercado_actual`. El pipeline no debe ejecutar Gold tras este fallo.

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
rejected_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver.rechazos_actuales")
# Puerta crítica: no sobrescribir la tabla Silver de mercado cuando el lote no es válido.
if rejected or accepted==0:
    notebookutils.fs.put(base+"/failure.json",json.dumps({"batch_id":batch_id,"received":received,"accepted":accepted,"rejected":rejected,"state":"BLOCKED_QUALITY"}),True)
    raise ValueError(f"Silver bloqueado: {rejected} filas inválidas de {received}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Construir el contrato Silver
# 
# Se seleccionan y renombran los campos de origen al vocabulario del informe: `id` pasa a `coin_id`, `current_price` a `precio_usd`, etc. `variacion_24h_pct` sigue en unidades porcentuales de CoinGecko: 1,5 significa 1,5 %, todavía no 0,015. Se añaden extracción UTC y lote, manteniendo separada la hora `actualizado_fuente_utc` de cada activo.

# CELL ********************

# Renombrar al contrato de negocio sin cambiar las unidades de los importes ni porcentajes.
silver = accepted_df.select(
    F.col("id").alias("coin_id"),F.col("name").alias("nombre"),F.col("symbol").alias("simbolo"),
    F.col("current_price").alias("precio_usd"),F.col("market_cap").alias("capitalizacion_usd"),
    F.col("market_cap_rank").alias("ranking_fuente"),F.col("total_volume").alias("volumen_24h_usd"),
    F.col("price_change_percentage_24h").alias("variacion_24h_pct"),F.col("fecha_fuente").alias("actualizado_fuente_utc")
).withColumn("extraido_utc",F.to_timestamp(F.lit(manifest["extracted_at_utc"]))).withColumn("batch_id",F.lit(batch_id))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Categoría de negocio de cada activo (menor prioridad = gana).
MAPA = {"tokenized-gold":("Tokenizados",1), "tokenized-treasuries":("Tokenizados",1),
        "tokenized-t-bills":("Tokenizados",1), "tokenized-money-market-fund-mmfs":("Tokenizados",1),
        "tokenized-credit":("Tokenizados",1), "tokenized-private-credit":("Tokenizados",1),
        "real-estate":("Tokenizados",1), "stablecoins":("Stablecoins",2)}
# Corrección manual (tiene prioridad sobre la API). Ejemplo: {"figure-heloc": "Tokenizados"}
EXCEPCIONES = {}

cat_raw = spark.table("bronze.categoria_raw").filter(F.col("batch_id")==batch_id)
if cat_raw.count() == 0:
    raise ValueError("No hay categorías Bronze para este lote; ejecutar Bronze completo primero")
mapa_df = spark.createDataFrame([(k, v[0], v[1]) for k, v in MAPA.items()],
                                "category_id string, categoria string, prioridad int")
asignada = (cat_raw.join(mapa_df, "category_id")
    .withColumn("rn", F.row_number().over(Window.partitionBy("coin_id").orderBy("prioridad","category_id")))
    .filter("rn = 1")
    .select("coin_id","categoria",F.concat(F.lit("API: "),F.col("category_id")).alias("categoria_fuente")))
silver = (silver.join(asignada, "coin_id", "left")
    .withColumn("categoria_fuente", F.when(F.col("categoria").isNull(), F.lit("Regla: sin otra categoría en la API"))
                                     .otherwise(F.col("categoria_fuente")))
    .withColumn("categoria", F.coalesce(F.col("categoria"), F.lit("Cripto"))))
for coin, cat in EXCEPCIONES.items():
    silver = (silver
        .withColumn("categoria_fuente", F.when(F.col("coin_id")==coin, F.lit("Excepción manual")).otherwise(F.col("categoria_fuente")))
        .withColumn("categoria", F.when(F.col("coin_id")==coin, F.lit(cat)).otherwise(F.col("categoria"))))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Reemplazar la foto actual y autorizar Gold
# 
# `overwrite` publica una foto completa validada, una fila por activo; no crea un histórico analítico de precios. `overwriteSchema` permite que la escritura aplique el esquema definido. Se comprueba el conteo persistido antes de escribir `silver_ready.json` con estado `READY`. Gold exigirá que ese permiso corresponda al mismo lote que el manifiesto actual.

# CELL ********************

# Snapshot completo validado: reemplazar el estado actual, conservando el crudo en Bronze.
silver.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver.mercado_actual")
written = spark.table("silver.mercado_actual").filter(F.col("batch_id")==batch_id).count()
if written != accepted:
    raise ValueError(f"Silver incompleto: {written} de {accepted} filas")
status = {"batch_id":batch_id,"received":received,"accepted":accepted,"rejected":rejected,"state":"READY"}
notebookutils.fs.put("Files/bronze/_control/coingecko/silver_ready.json",json.dumps(status),True)
print(json.dumps({"fase":"Silver",**status},ensure_ascii=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
