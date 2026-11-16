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

# # Gold · Modelo en estrella, trazabilidad y modelo semántico
# 
# **Objetivo de esta capa: Gold.** Caso docente CoinGecko, muestra de hasta
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

# Los parámetros del lote se leen de Bronze y del estado Silver.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Herramientas y contexto
# 
# Fabric proporciona `spark`. PySpark construye dimensiones y hechos; `Window` ordena el ranking y deduplica el histórico. `requests` se reserva para refrescar el modelo. Todas las fechas usan UTC.

# CELL ********************

import json, time, requests
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import notebookutils

# Las conversiones de timestamp se interpretan en UTC durante toda la sesión.
spark.conf.set("spark.sql.session.timeZone", "UTC")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Lote Silver autorizado
# 
# Entrada: manifiesto Bronze y estado Silver READY del mismo lote. Se exige una fila por activo, el conteo esperado y capitalización positiva. Se conserva la ruta original bajo Files/bronze. Todavía no se publica ninguna tabla ni se actualiza Power BI.

# CELL ********************

# PASO: lote
manifest = json.loads(notebookutils.fs.head("Files/bronze/_control/coingecko/current_batch.json",1048576))
status = json.loads(notebookutils.fs.head("Files/bronze/_control/coingecko/silver_ready.json",1048576))
batch_id = manifest["batch_id"]
if status["batch_id"] != batch_id or status["state"] != "READY":
    raise ValueError("Silver no validó el lote actual")
started, mode = manifest["processed_at_utc"], manifest["mode"]
base = manifest["raw_path"].rsplit("/",1)[0]
received, accepted, rejected = status["received"], status["accepted"], status["rejected"]
top_n = int(manifest["parameters"]["per_page"])
silver = spark.table("silver.mercado_actual").filter(F.col("batch_id")==batch_id).cache()
if silver.count()!=accepted or accepted==0 or silver.select("coin_id").distinct().count()!=accepted:
    raise ValueError("Silver no cumple el grano de un activo por lote")
total_cap = silver.agg(F.sum("capitalizacion_usd")).first()[0]
if total_cap is None or total_cap<=0:
    raise ValueError("La capitalización de la muestra no es positiva")
published = datetime.now(timezone.utc).isoformat()
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Recuperar la auditoría sin duplicar lotes
# 
# Se conserva el histórico existente de publicaciones. La primera ejecución migra la auditoría anterior; las siguientes leen la estrella nueva. Una fila por batch_id evita duplicar auditoría si se repite Gold. Se usa una clave numérica estable de lote y se comprobará su unicidad. Las extracciones históricas desconocidas quedan nulas, sin sustituirlas por una fecha inventada.

# CELL ********************

# PASO: historial
# La primera migración recupera la auditoría existente. Después la fuente canónica
# son las dimensiones y hechos nuevos; no se depende de la tabla plana antigua.
run_schema = "batch_id string, inicio_utc string, fin_utc string, estado string, filas_recibidas long, filas_aceptadas long, filas_rechazadas long, moneda string, tamano_solicitado long, ruta_bronze string, modo_datos string, entorno string, extraido_utc string"
run_df = spark.createDataFrame([(batch_id,started,published,"PUBLICADO",received,accepted,rejected,
    "usd",top_n,base,mode,"Fabric Notebook",manifest["extracted_at_utc"])],run_schema)
if spark.catalog.tableExists("gold.dim_lote") and spark.catalog.tableExists("gold.fact_ejecuciones"):
    previous = spark.table("gold.dim_lote").join(spark.table("gold.fact_ejecuciones"),"lote_key")
    history = previous.select(*[F.col(field.name).cast(field.dataType).alias(field.name)
                                for field in run_df.schema.fields])
elif spark.catalog.tableExists("gold.ejecuciones"):
    # El histórico anterior no guardaba extracción UTC: se deja nula, no se inventa.
    history = spark.table("gold.ejecuciones").withColumn("extraido_utc",F.lit(None).cast("string"))
    history = history.select(run_df.columns)
else:
    history = spark.createDataFrame([],run_schema)
runs = history.unionByName(run_df)
runs = (runs.withColumn("_version",F.row_number().over(Window.partitionBy("batch_id").orderBy(F.to_timestamp("fin_utc").desc())))
        .filter("_version = 1").drop("_version")
        .withColumn("lote_key",F.xxhash64(F.lit("lote"),F.col("batch_id")))
        .withColumn("fecha_key",F.date_format(F.to_timestamp("inicio_utc"),"yyyyMMdd").cast("long")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Dimensiones Activo, Lote y Control
# 
# `dim_activo`: una fila por coin_id con nombre y símbolo. Clave estable xxhash64 y atributos tipo 1 de la muestra actual. `dim_lote`: una fila por publicación con sus atributos de contexto y bandera es_actual. `dim_control`: una fila por regla con fase y descripción. Los conteos y observaciones numéricas se reservan para hechos. Las dimensiones no se relacionan entre sí.

# CELL ********************

# PASO: dimensiones
# Activo es una dimensión de tipo 1 de la muestra actual, no un histórico SCD2.
# xxhash64 crea claves estables al reordenar filas. Se comprobarán sus colisiones.
dim_activo = (silver.select("coin_id","nombre","simbolo","categoria","categoria_fuente")
              .withColumn("activo_key",F.xxhash64(F.lit("activo"),F.col("coin_id")))
              .select("activo_key","coin_id","nombre","simbolo","categoria","categoria_fuente"))
dim_lote = runs.select("lote_key","batch_id",
    F.to_timestamp("inicio_utc").alias("inicio_utc"),F.to_timestamp("fin_utc").alias("fin_utc"),
    F.to_timestamp("extraido_utc").alias("extraido_utc"),"estado","moneda",
    F.regexp_replace("ruta_bronze","^Files/landing/","Files/bronze/").alias("ruta_bronze"),
    "modo_datos","entorno",F.lit("CoinGecko real · Fabric").alias("origen"),
    (F.col("batch_id")==F.lit(batch_id)).alias("es_actual"))
missing_changes = silver.filter(F.col("variacion_24h_pct").isNull()).count()
cat_invalidas = silver.filter(F.col("categoria").isNull() |
                              ~F.col("categoria").isin("Cripto","Stablecoins","Tokenizados")).count()
control_rows = [
    (1,"C01","Bronze","Colección no vacía",received,1,"OK"),
    (2,"C02","Bronze","Filas solicitadas",received,top_n,"OK" if received==top_n else "AVISO"),
    (3,"C03","Silver","Filas inválidas",rejected,0,"OK"),
    (4,"C04","Silver","Claves únicas",accepted,accepted,"OK"),
    (5,"C05","Silver","Reconciliación",accepted+rejected,received,"OK"),
    (6,"C06","Silver","Variaciones 24h ausentes",missing_changes,0,"OK" if missing_changes==0 else "AVISO"),
    (7,"C07","Gold","Filas publicadas",accepted,accepted,"OK"),
    (8,"C08","Gold","Categorías válidas",accepted-cat_invalidas,accepted,"OK" if cat_invalidas==0 else "AVISO")]

    
controls = spark.createDataFrame(control_rows,
    "control_key long, control_id string, fase string, regla string, observados long, umbral long, resultado string")
dim_control = controls.select("control_key","control_id","fase","regla")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Dimensión Fecha continua
# 
# Se generan todos los días de los años cubiertos por la auditoría, con clave YYYYMMDD, fecha, año, trimestre, mes y día. Mes se ordena por mes_numero. Es el día UTC de procesamiento del lote; extracción y actualización del activo conservan sus propios timestamps. Tener calendario no crea un histórico de precios: el mercado sigue siendo una foto actual.

# CELL ********************

# PASO: calendario
# El calendario representa el día UTC de procesamiento: inicio_utc del lote.
# Una repetición puede conservar extracción antigua pero tener otro día de proceso.
bounds = runs.agg(F.min("inicio_utc").alias("first"),F.max("inicio_utc").alias("last")).first()
first_year, last_year = str(bounds["first"])[:4], str(bounds["last"])[:4]
dates = spark.range(1).select(F.explode(F.sequence(
    F.lit(first_year+"-01-01").cast("date"),F.lit(last_year+"-12-31").cast("date"),
    F.expr("interval 1 day"))).alias("fecha"))
months = F.array(*[F.lit(x) for x in ["Enero","Febrero","Marzo","Abril","Mayo","Junio", 
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]])
dim_fecha = dates.select(F.date_format("fecha","yyyyMMdd").cast("long").alias("fecha_key"),
    "fecha",F.year("fecha").cast("long").alias("anio"),F.quarter("fecha").cast("long").alias("trimestre"),
    F.month("fecha").cast("long").alias("mes_numero"),F.element_at(months,F.month("fecha")).alias("mes"),
    F.dayofmonth("fecha").cast("long").alias("dia"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Hechos y grano de cada tabla
# 
# `fact_mercado_actual`: una fila por activo y lote actual, con claves a Activo, Fecha y Lote. Contiene precio, capitalización, volumen, variación, posiciones y participación; no nombres ni símbolos. `fact_ejecuciones`: una fila por lote histórico, con conteos de carga y claves a Fecha y Lote. `fact_controles_actuales`: una fila por regla y lote actual con observado, umbral y resultado. Ningún hecho se relaciona directamente con otro. Precio, variación y proporciones no se suman como ingresos; las medidas del modelo definen su agregación.

# CELL ********************

# PASO: hechos
current_lote = dim_lote.filter(F.col("batch_id")==batch_id).select("lote_key").first()[0]
current_fecha = int(datetime.fromisoformat(started.replace("Z","+00:00")).strftime("%Y%m%d"))
ranked = (silver.withColumn("posicion_muestra",F.row_number().over(
    Window.orderBy(F.desc("capitalizacion_usd"),F.asc("coin_id"))))
    .withColumn("participacion_muestra",F.col("capitalizacion_usd")/F.lit(total_cap)))
# El hecho contiene claves, métricas y la fecha de observación, sin nombre/símbolo
# ni atributos descriptivos del lote. Precio y variación NO son métricas aditivas.
fact_mercado = (ranked.join(dim_activo.select("coin_id","activo_key"),"coin_id")
    .select("activo_key",F.lit(current_lote).cast("long").alias("lote_key"),
        F.lit(current_fecha).cast("long").alias("fecha_key"),"precio_usd","capitalizacion_usd",
        "volumen_24h_usd","variacion_24h_pct","ranking_fuente","posicion_muestra",
        "participacion_muestra","actualizado_fuente_utc"))
fact_ejecuciones = runs.select("lote_key","fecha_key","filas_recibidas","filas_aceptadas",
    "filas_rechazadas","tamano_solicitado")
fact_controles = controls.select("control_key",F.lit(current_lote).cast("long").alias("lote_key"),
    F.lit(current_fecha).cast("long").alias("fecha_key"),"observados","umbral","resultado")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Comprobar claves, grano e integridad referencial
# 
# Antes de escribir se comprueban claves no nulas y únicas de las cuatro dimensiones, incluido cualquier choque de hash. Se validan los tres granos y las ocho relaciones mediante anti-join: ninguna clave foránea puede quedar sin dimensión. Se concilian las filas y la capitalización con Silver. Ante un fallo se detiene el notebook antes de reemplazar la estrella.

# CELL ********************

# PASO: integridad
def require_unique(frame, columns, name):
    if frame.filter(F.expr(" OR ".join(c+" IS NULL" for c in columns))).limit(1).count():
        raise ValueError(name+": clave nula")
    if frame.groupBy(*columns).count().filter("count > 1").limit(1).count():
        raise ValueError(name+": clave repetida o colisión de hash")

for frame,key,name in [(dim_activo,"activo_key","Activo"),(dim_lote,"lote_key","Lote"),
                       (dim_fecha,"fecha_key","Fecha"),(dim_control,"control_key","Control")]:
    require_unique(frame,[key],name)
require_unique(fact_mercado,["activo_key","lote_key"],"Grano Mercado")
require_unique(fact_ejecuciones,["lote_key"],"Grano Ejecuciones")
require_unique(fact_controles,["control_key","lote_key"],"Grano Controles")
relationships = [("fact_mercado_actual",fact_mercado,"activo_key",dim_activo),
    ("fact_mercado_actual",fact_mercado,"fecha_key",dim_fecha),
    ("fact_mercado_actual",fact_mercado,"lote_key",dim_lote),
    ("fact_ejecuciones",fact_ejecuciones,"fecha_key",dim_fecha),
    ("fact_ejecuciones",fact_ejecuciones,"lote_key",dim_lote),
    ("fact_controles_actuales",fact_controles,"control_key",dim_control),
    ("fact_controles_actuales",fact_controles,"fecha_key",dim_fecha),
    ("fact_controles_actuales",fact_controles,"lote_key",dim_lote)]
for name,fact,key,dimension in relationships:
    if fact.filter(F.col(key).isNull()).limit(1).count() or fact.join(dimension.select(key),key,"left_anti").limit(1).count():
        raise ValueError(name+": referencia huérfana en "+key)
if fact_mercado.count()!=accepted or fact_controles.count()!=8:
    raise ValueError("Los hechos no contienen las filas esperadas")
if abs(fact_mercado.agg(F.sum("capitalizacion_usd")).first()[0]-total_cap)>0.01:
    raise ValueError("La capitalización no concilia con Silver")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Persistir tablas dimensionales Delta
# 
# Las siete tablas se materializan con localCheckpoint(eager=True), que corta su dependencia de las tablas leídas antes de sobrescribirlas. El almacenamiento temporal pertenece a la sesión Spark; no crea otra carpeta raíz en Files. Cada tabla usa overwrite; fact_ejecuciones conserva su histórico porque previamente lo ha incorporado y deduplicado. Las dimensiones preceden a los hechos. Delta protege cada escritura, no el conjunto: se ejecuta una sola carga a la vez y el modelo solo se refresca después de completar Gold.

# CELL ********************

# PASO: persistir
tables = {"dim_activo":dim_activo,"dim_fecha":dim_fecha,"dim_lote":dim_lote,"dim_control":dim_control,
    "fact_mercado_actual":fact_mercado,"fact_ejecuciones":fact_ejecuciones,"fact_controles_actuales":fact_controles}
# Materializar antes de sobrescribir evita releer dimensiones/hechos que son el
# origen del histórico durante esta misma ejecución (Spark evalúa con pereza).
tables = {name:frame.localCheckpoint(eager=True) for name,frame in tables.items()}
for name,frame in tables.items():
    frame.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold."+name)
    if spark.table("gold."+name).count()!=frame.count():
        raise ValueError("Conteo persistido incorrecto: "+name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Trazabilidad documental independiente
# 
# Las 32 filas documentales derivan del Excel. Son una tabla de utilidad para explicar requisitos, no una dimensión de mercado ni un hecho de ejecución. Se mantiene intencionadamente desconectada para que un filtro sobre la documentación no altere métricas de mercado.

# CELL ********************

trace_rows = [{'hoja': 'Tipo de carga',
  'columna': 'A',
  'criterio': '#',
  'pregunta': '¿Qué iniciativa entregamos?',
  'fase': 'Diseño',
  'artefacto': 'Documentación',
  'aplicacion': 'Caso 1: observatorio de activos digitales.',
  'evidencia': 'Identificador del caso y alcance.'},
 {'hoja': 'Tipo de carga',
  'columna': 'B',
  'criterio': 'Proyecto',
  'pregunta': '¿Qué necesidad resolvemos?',
  'fase': 'Gold',
  'artefacto': 'Power BI',
  'aplicacion': 'Dar al analista una foto diaria del universo observado.',
  'evidencia': 'Preguntas PN-01 a PN-09.'},
 {'hoja': 'Tipo de carga',
  'columna': 'C',
  'criterio': 'Tipo de carga',
  'pregunta': '¿Cómo mantenemos la foto actual?',
  'fase': 'Silver / Gold',
  'artefacto': 'Notebooks',
  'aplicacion': 'Full load del top 100 y overwrite de la versión actual, tras validar.',
  'evidencia': 'Segunda carga sin duplicar filas.'},
 {'hoja': 'Tipo de carga',
  'columna': 'D',
  'criterio': 'API pública (URL)',
  'pregunta': '¿De dónde proceden los datos?',
  'fase': 'Bronze',
  'artefacto': 'Ingesta REST',
  'aplicacion': 'CoinGecko /coins/markets, USD, página 1, top 100.',
  'evidencia': 'JSON original, URL y fecha UTC.'},
 {'hoja': 'Tipo de carga',
  'columna': 'E',
  'criterio': '¿Gratis?',
  'pregunta': '¿Qué costes debemos separar?',
  'fase': 'Transversal',
  'artefacto': 'Capacidad Fabric',
  'aplicacion': 'El acceso a una API y el cómputo de Fabric tienen condiciones distintas.',
  'evidencia': 'Plan de API y consumo de capacidad.'},
 {'hoja': 'Tipo de carga',
  'columna': 'F',
  'criterio': 'Fuente gratis sin tarjeta recomendada',
  'pregunta': '¿Qué fuente real usamos?',
  'fase': 'Bronze',
  'artefacto': 'Ingesta REST',
  'aplicacion': 'CoinGecko real. El acceso sin clave funcionó en la extracción local; no garantiza '
                'disponibilidad futura.',
  'evidencia': 'HTTP 200 y 100 filas de la extracción.'},
 {'hoja': 'Tipo de carga',
  'columna': 'G',
  'criterio': 'Límites de uso / condiciones',
  'pregunta': '¿Qué pasa si la API limita llamadas?',
  'fase': 'Bronze',
  'artefacto': 'Ingesta REST',
  'aplicacion': 'Respetar límites del plan y Retry-After; reintentos acotados, nunca publicar una '
                'respuesta de error.',
  'evidencia': 'Estado HTTP y registro de reintentos.'},
 {'hoja': 'Tipo de carga',
  'columna': 'H',
  'criterio': 'Herramientas / Tecnologías Microsoft Fabric',
  'pregunta': '¿Quién hace cada trabajo?',
  'fase': 'Transversal',
  'artefacto': 'Lakehouse / Notebooks / Pipeline / Power BI',
  'aplicacion': 'Un notebook por capa: Bronze ingiere, Silver valida, Gold publica; el pipeline '
                'orquesta y avisa.',
  'evidencia': 'Tres notebooks y sus dependencias desplegados.'},
 {'hoja': 'Tipo de carga',
  'columna': 'I',
  'criterio': 'Nota breve',
  'pregunta': '¿Qué se reemplaza?',
  'fase': 'Silver / Gold',
  'artefacto': 'Delta',
  'aplicacion': 'Silver y Gold actuales se reemplazan; Landing y Bronze conservan cada extracción.',
  'evidencia': 'JSON original, lotes Bronze y tabla actual.'},
 {'hoja': 'Tipo de carga',
  'columna': 'J',
  'criterio': 'Origen típico',
  'pregunta': '¿Qué clase de fuente es?',
  'fase': 'Bronze',
  'artefacto': 'API REST',
  'aplicacion': 'Respuesta JSON del estado actual; no es una captura CDC.',
  'evidencia': 'Inspección del payload.'},
 {'hoja': 'Tipo de carga',
  'columna': 'K',
  'criterio': 'Criterios de evaluación (resumen)',
  'pregunta': '¿Por qué elegimos carga completa?',
  'fase': 'Diseño',
  'artefacto': 'Arquitectura',
  'aplicacion': 'Solo 100 filas, cadencia diaria y objetivo de simplicidad. Medir antes de '
                'escalar.',
  'evidencia': 'Volumen y duración de ejecuciones.'},
 {'hoja': 'Tipo de carga',
  'columna': 'L',
  'criterio': 'Cuándo usar',
  'pregunta': '¿En qué casos es adecuado?',
  'fase': 'Diseño',
  'artefacto': 'Arquitectura',
  'aplicacion': 'Conjunto acotado y necesidad de último estado, sin watermark fiable de la '
                'colección.',
  'evidencia': 'Contrato top 100 / USD.'},
 {'hoja': 'Tipo de carga',
  'columna': 'M',
  'criterio': 'Cuándo evitar',
  'pregunta': '¿Cuándo cambiaríamos de estrategia?',
  'fase': 'Diseño',
  'artefacto': 'Arquitectura',
  'aplicacion': 'Si crece el volumen, se exige baja latencia o se necesita reconstruir cada cambio '
                'de negocio.',
  'evidencia': 'Revisión de SLA y volumen.'},
 {'hoja': 'Tipo de carga',
  'columna': 'N',
  'criterio': 'Volumen recomendado',
  'pregunta': '¿Cuánto cargamos?',
  'fase': 'Bronze',
  'artefacto': 'Ingesta / Lakehouse',
  'aplicacion': 'Solicitar hasta 100 activos y medir filas y bytes reales; no asumir cobertura '
                'global.',
  'evidencia': 'Conteo y tamaño JSON.'},
 {'hoja': 'Tipo de carga',
  'columna': 'O',
  'criterio': 'Latencia objetivo',
  'pregunta': '¿Cuánto retraso admite el cliente?',
  'fase': 'Transversal',
  'artefacto': 'Pipeline / Power BI',
  'aplicacion': 'Propuesta: lote diario visible antes de 09:00 Europe/Madrid; objetivo de proceso '
                '15 minutos a validar.',
  'evidencia': 'Inicio, fin y fecha de publicación.'},
 {'hoja': 'Tipo de carga',
  'columna': 'P',
  'criterio': 'Frecuencia típica',
  'pregunta': '¿Cada cuánto actualizamos?',
  'fase': 'Bronze → Gold',
  'artefacto': 'Pipeline',
  'aplicacion': 'El diseño admite una carga diaria; para la charla se mantiene manual, sin horario '
                'activo.',
  'evidencia': 'Historial y lista vacía de programaciones.'},
 {'hoja': 'Tipo de carga',
  'columna': 'Q',
  'criterio': 'Actualizaciones/Borrado',
  'pregunta': '¿Qué significa que una moneda desaparezca?',
  'fase': 'Silver / Gold',
  'artefacto': 'Overwrite Delta',
  'aplicacion': 'La tabla refleja la muestra actual; salir del top 100 no prueba que el activo '
                'haya sido eliminado.',
  'evidencia': 'Comparación de conjuntos de dos lotes.'},
 {'hoja': 'Tipo de carga',
  'columna': 'R',
  'criterio': 'Costo relativo',
  'pregunta': '¿Qué encarece la solución?',
  'fase': 'Transversal',
  'artefacto': 'Capacidad Fabric',
  'aplicacion': 'La carga completa escala con N; para 100 filas puede pesar más el arranque de '
                'Spark. Sin presupuesto inventado.',
  'evidencia': 'Duración y métricas de capacidad.'},
 {'hoja': 'Tipo de carga',
  'columna': 'S',
  'criterio': 'Tecnologías/Patrones Fabric',
  'pregunta': '¿Qué conceptos aplicamos de verdad?',
  'fase': 'Bronze / Silver / Gold',
  'artefacto': 'Delta / Notebooks',
  'aplicacion': 'Medallion + ELT; Bronze append, Silver y Gold overwrite. Un notebook por capa.',
  'evidencia': 'Código, tablas y pipeline del caso.'},
 {'hoja': 'Tipo de carga',
  'columna': 'T',
  'criterio': 'Buenas prácticas clave',
  'pregunta': '¿Cómo prevenimos datos incorrectos?',
  'fase': 'Bronze → Gold',
  'artefacto': 'Notebooks / auditoría',
  'aplicacion': 'Metadatos de lote, tipos explícitos, controles previos y evidencia de '
                'publicación. Sin particionar 100 filas.',
  'evidencia': 'Controles y conteos reconciliados.'},
 {'hoja': 'Tipo de carga',
  'columna': 'U',
  'criterio': 'Riesgos/Notas',
  'pregunta': '¿Qué puede impedir publicar?',
  'fase': 'Transversal',
  'artefacto': 'Pipeline / auditoría',
  'aplicacion': 'HTTP 429, cambio de esquema, respuesta vacía, claves duplicadas o datos obsoletos '
                'bloquean la publicación según gravedad.',
  'evidencia': 'Estado fallido y último lote válido identificado.'},
 {'hoja': 'Tipo de carga',
  'columna': 'V',
  'criterio': 'Documentación oficial (Microsoft)',
  'pregunta': '¿Cómo justificamos las decisiones?',
  'fase': 'Diseño',
  'artefacto': 'Documentación',
  'aplicacion': 'Contrastar cada comportamiento con Microsoft Learn y CoinGecko. El Excel es '
                'material de partida.',
  'evidencia': 'Referencias al final del Word.'},
 {'hoja': 'Tipo de carga',
  'columna': 'W',
  'criterio': 'Fecha live YouTube',
  'pregunta': '¿Cuándo se presenta?',
  'fase': 'Entrega',
  'artefacto': 'Guion',
  'aplicacion': 'Lunes 2 de noviembre de 2026, 60 minutos, iniciación.',
  'evidencia': 'Agenda y ensayo previo.'},
 {'hoja': 'Arquitectura',
  'columna': 'A',
  'criterio': 'Arquitectura',
  'pregunta': '¿Dónde organizamos el dato?',
  'fase': 'Transversal',
  'artefacto': 'Lakehouse',
  'aplicacion': 'Un Lakehouse con esquemas bronze, silver y gold, más Files/bronze para el JSON '
                'original.',
  'evidencia': 'Árbol Files y Tables.'},
 {'hoja': 'Arquitectura',
  'columna': 'B',
  'criterio': '¿Qué es una Arquitectura?',
  'pregunta': '¿Qué estructura conecta el origen con el negocio?',
  'fase': 'Transversal',
  'artefacto': 'Lakehouse / modelo / informe',
  'aplicacion': 'Describe componentes y responsabilidades; no es una lista de herramientas '
                'sueltas.',
  'evidencia': 'Diagrama del recorrido completo.'},
 {'hoja': 'Arquitectura',
  'columna': 'C',
  'criterio': 'Metodologías',
  'pregunta': '¿Cómo trabajamos los datos?',
  'fase': 'Bronze → Gold',
  'artefacto': 'ELT con notebooks',
  'aplicacion': 'Extraer, cargar la respuesta original y transformar dentro de Fabric.',
  'evidencia': 'Orden de actividades.'},
 {'hoja': 'Arquitectura',
  'columna': 'D',
  'criterio': '¿Qué es una Metodología? (Profundización)',
  'pregunta': '¿Qué aporta cada paso?',
  'fase': 'Bronze → Gold',
  'artefacto': 'Notebooks',
  'aplicacion': 'Obtener evidencia, validar y modelar. MDM, mirroring y virtualización no se '
                'requieren en esta fuente.',
  'evidencia': 'Entradas y salidas por notebook.'},
 {'hoja': 'Arquitectura',
  'columna': 'E',
  'criterio': 'Patrones aplicables',
  'pregunta': '¿Qué prácticas técnicas elegimos?',
  'fase': 'Silver / Gold',
  'artefacto': 'Delta',
  'aplicacion': 'Medallion para calidad y full load / overwrite para actualización; son decisiones '
                'diferentes.',
  'evidencia': 'Contrato y estrategia de escritura.'},
 {'hoja': 'Arquitectura',
  'columna': 'F',
  'criterio': '¿Qué es un Patrón? (Profundización)',
  'pregunta': '¿Cómo evitamos confundir conceptos?',
  'fase': 'Silver / Gold',
  'artefacto': 'Spark / Delta / SQL endpoint',
  'aplicacion': 'Delta es formato de tablas; Spark transforma; el SQL endpoint consulta; Gold '
                'representa negocio.',
  'evidencia': 'Mismo dato observado desde cada artefacto.'},
 {'hoja': 'Arquitectura',
  'columna': 'G',
  'criterio': '¿Medallion?',
  'pregunta': '¿Cómo progresa la preparación?',
  'fase': 'Bronze → Silver → Gold',
  'artefacto': 'Lakehouse / notebooks',
  'aplicacion': 'Landing conserva JSON; Bronze Delta registra cada fila; Silver valida; Gold '
                'responde preguntas.',
  'evidencia': 'Mismo activo a través de tres esquemas.'},
 {'hoja': 'Arquitectura',
  'columna': 'H',
  'criterio': 'Casos de negocio (8 casos profundizados)',
  'pregunta': '¿Qué preguntas responde esta arquitectura?',
  'fase': 'Gold',
  'artefacto': 'Power BI',
  'aplicacion': 'Aplicamos la estructura al observatorio de criptomonedas; ventas y churn del '
                'Excel son otros ejemplos, no datos de CoinGecko.',
  'evidencia': 'PN-01 a PN-09 y sus visuales.'},
 {'hoja': 'Arquitectura',
  'columna': 'I',
  'criterio': 'Artefactos de Microsoft Fabric',
  'pregunta': '¿Qué mostramos en la primera charla?',
  'fase': 'Transversal',
  'artefacto': 'Lakehouse / Notebooks / Pipeline / modelo / informe',
  'aplicacion': 'Estos cinco componentes sostienen el recorrido; SQL endpoint se explica como '
                'superficie de consulta.',
  'evidencia': 'Inventario y navegación en Fabric.'}]
trace_df=spark.createDataFrame(trace_rows)
trace_df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('gold.trazabilidad')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Evidencia de la estrella y conciliación
# 
# Se exporta cada tabla bajo Files/bronze/_publicacion/export_estrella y se registran sus conteos y las ocho relaciones comprobadas. Estas copias son pequeñas evidencias de la demo; el modelo semántico, en modo Importar, lee las tablas Gold a través del punto de conexión SQL del Lakehouse. `collect` solo se utiliza por el pequeño volumen del caso.

# CELL ********************

# PASO: exportar
export_base = "Files/bronze/_publicacion/export_estrella/"+batch_id
notebookutils.fs.mkdirs(export_base)
tables["trazabilidad"] = trace_df
# Son evidencias derivadas, no el original: permitir reemplazo hace recuperable
# la publicación del mismo lote sin duplicar su auditoría ni sus claves.
for name,frame in tables.items():
    notebookutils.fs.put(export_base+"/"+name+".json","["+",".join(frame.toJSON().collect())+"]",True)
summary = {"batch_id":batch_id,"export_base":export_base,"received":received,"accepted":accepted,
    "rejected":rejected,"capitalizacion_usd":total_cap,
    "concentracion_top10":fact_mercado.filter("posicion_muestra <= 10").agg(F.sum("participacion_muestra")).first()[0],
    "published_at_utc":published,"mode":mode,"modelo_gold":"estrella_v1",
    "filas_tablas":{name:frame.count() for name,frame in tables.items()},"relaciones_verificadas":len(relationships)}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Refrescar el modelo semántico
# 
# La identidad de Fabric obtiene un token pbi, encuentra el modelo por nombre y solicita su refresco. Se espera Completed, propagando fallo o tiempo agotado. La solicitud aceptada no basta para declarar éxito. El token no se escribe en las evidencias. Las relaciones 1:N y medidas se despliegan en el modelo semántico; un notebook Spark no las crea por escribir tablas.

# CELL ********************

context = notebookutils.runtime.context
workspace_id = context["currentWorkspaceId"]
pbi_headers = {"Authorization":"Bearer " + notebookutils.credentials.getToken("pbi")}
models_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
models_response = requests.get(models_url,headers=pbi_headers,timeout=60)
models_response.raise_for_status()
models = [x for x in models_response.json()["value"] if x["name"]=="PBI_CoinGecko"]
if len(models)!=1: raise ValueError("Se esperaba un único modelo semántico llamado PBI_CoinGecko")
model_id = models[0]["id"]
refresh_url = models_url + "/" + model_id + "/refreshes"
# Actualizar el modelo solo después de completar las escrituras Gold.
# Actualizar el modelo solo después de completar las escrituras Gold.
refresh_response = requests.post(refresh_url,headers=pbi_headers,json={"type":"full","commitMode":"transactional","retryCount":0},timeout=60)
refresh_response.raise_for_status()
refresh_location=refresh_response.headers.get("Location")
if not refresh_location:
    raise ValueError("El servicio no devolvió la ubicación de actualización; comprobar antes de repetir.")
for attempt in range(90):
    refresh_state=requests.get(refresh_location,headers=pbi_headers,timeout=60)
    refresh_state.raise_for_status()
    refresh_data=refresh_state.json()
    # La petición aceptada no basta: esperar a que el servicio confirme la finalización.
    # La petición aceptada no basta: esperar a que el servicio confirme la finalización.
    if refresh_data.get("status")=="Completed":break
    if refresh_data.get("status") in ["Failed","Cancelled","Disabled"]:
        raise RuntimeError(json.dumps(refresh_data))
    time.sleep(5)
else:
    raise TimeoutError("La actualización del modelo sigue pendiente; no se declara completada.")
notebookutils.fs.put("Files/bronze/_publicacion/model_refresh.json",json.dumps({"batch_id":batch_id,"semantic_model_id":model_id,"refresh_location":refresh_location,"result":refresh_data}),True)
print("Modelo actualizado:",model_id,refresh_data["status"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Edición pública sin retirada de tablas
# 
# Esta copia no borra tablas antiguas ni necesita migrar el entorno de la charla.
# Usa un Lakehouse propio y dedicado al caso. La estrella nueva conserva su auditoría por lote.


# CELL ********************

# Edición pública: no se eliminan tablas en el entorno del participante.
summary["tablas_planas_retiradas"] = []


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Confirmar la publicación completa
# 
# Solo al finalizar el refresco se actualiza latest.json y la marca de éxito junto al original. El pipeline podrá enviar su aviso final. El resumen distingue lote, modo real o replay, conteos y versión dimensional; Monitor conserva el estado operativo de la ejecución.

# CELL ********************

notebookutils.fs.put(base+"/success.json",json.dumps(summary),True)
notebookutils.fs.put("Files/bronze/_publicacion/latest.json",json.dumps(summary),True)
print(json.dumps({"fase":"Gold",**summary,"model_refresh":"Completed"},ensure_ascii=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
