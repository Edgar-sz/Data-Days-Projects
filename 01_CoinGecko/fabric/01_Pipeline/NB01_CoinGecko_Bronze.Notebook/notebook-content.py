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

# # Bronze · JSON original en Files y tabla Delta append-only
# 
# **Objetivo de esta capa: Bronze.** Caso docente CoinGecko, muestra de hasta
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


# MARKDOWN ********************

# ## Parámetros de ejecución
# 
# `top_n=100` solicita una sola página de hasta 100 activos. `replay_path=""` consulta la API real; una ruta a un JSON conservado repite ese archivo y necesita su `manifest.json` vecino. La actividad de pipeline puede sustituir estos valores mediante su celda de parámetros.

# PARAMETERS CELL ********************

replay_path = ""
top_n = 100


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Librerías, sesión y lote
# 
# `requests` consume HTTP; `json` conserva y lee respuestas; `uuid` evita colisiones entre lotes. `datetime` genera fechas UTC. `F` y `T` son utilidades PySpark. La configuración UTC evita interpretar las fechas según la zona de la sesión. `batch_id` combina instante UTC y un sufijo aleatorio: repetir un archivo genera otro intento auditable, no reutiliza el lote anterior. `started` es el inicio de este procesamiento; no es la fecha de actualización de CoinGecko.

# CELL ********************

import json, uuid, time, requests
from datetime import datetime, timezone
from pyspark.sql import functions as F, types as T
import notebookutils

# Las conversiones de timestamp se interpretan en UTC durante toda la sesión.
spark.conf.set("spark.sql.session.timeZone", "UTC")
# Identificador del intento; una repetición crea otro lote para conservar su auditoría.
batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
started = datetime.now(timezone.utc).isoformat()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Origen y alcance de la consulta
# 
# `/coins/markets` devuelve atributos de activos. `vs_currency=usd` fija unidades; `market_cap_desc` ordena por capitalización; `per_page` limita el tamaño; `page=1` lee solo la primera página; `sparkline=false` evita traer series que no usa el informe. Solicitar 100 filas no garantiza recibir 100. Esta demo no pagina todo el mercado.

# CELL ********************

url = "https://api.coingecko.com/api/v3/coins/markets"
params = {"vs_currency":"usd", "order":"market_cap_desc", "per_page":int(top_n), "page":1, "sparkline":"false"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Extracción real o repetición de un archivo
# 
# Con `replay_path` se lee el JSON conservado y su manifiesto vecino. El límite de `head` es 10 MiB para el JSON y 1 MiB para el manifiesto, suficiente para esta muestra, no para cargas masivas. Se conserva `extracted_at_utc` original y se etiqueta `REPLAY_REAL`. Sin ruta se consulta CoinGecko, con 15 s para conectar y 60 s para leer. Solo se reintentan 429 y ciertos 5xx, hasta tres intentos y una espera acotada. `raise_for_status` detiene el proceso ante una respuesta HTTP de error. `payload` guarda el texto recibido; `data` es su interpretación como objetos Python. `LIVE_REAL` identifica la consulta nueva.

# CELL ********************

if replay_path:
    payload = notebookutils.fs.head(replay_path, 10485760)
    data = json.loads(payload)
    original_manifest = json.loads(notebookutils.fs.head(replay_path.rsplit("/",1)[0]+"/manifest.json",1048576))
    extracted = original_manifest["extracted_at_utc"]
    mode = "REPLAY_REAL"
else:
    for attempt in range(3):
        # Separamos tiempo máximo de conexión y lectura; esta llamada no escribe datos.
        response = requests.get(url, params=params, timeout=(15,60), headers={"Accept":"application/json"})
        if response.status_code not in (429,500,502,503,504):
            break
        if attempt < 2:
            time.sleep(min(int(response.headers.get("Retry-After", 5 * (2**attempt))),30))
    # Detener ante HTTP de error; nunca tratar una respuesta de error como mercado.
    response.raise_for_status()
    payload = response.text
    data = response.json()
    extracted = datetime.now(timezone.utc).isoformat()
    mode = "LIVE_REAL"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Contrato mínimo antes de almacenar
# 
# La respuesta debe ser una lista no vacía y cada objeto debe incluir los campos críticos. Este control comprueba estructura, todavía no rangos ni tipos numéricos. Ante un incumplimiento se lanza `ValueError` y no se escribe Bronze. Por tanto, una respuesta HTTP errónea o un payload sin el contrato mínimo no quedan archivados por este notebook.

# CELL ********************

if not isinstance(data,list) or not data:
    raise ValueError("La API no devolvió una lista no vacía; no se publica.")
required = {"id","name","current_price","market_cap","total_volume","last_updated"}
if any(not isinstance(row,dict) or not required.issubset(row) for row in data):
    raise ValueError("Faltan campos críticos en el JSON; no se publica.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. JSON original y manifiesto del lote
# 
# La carpeta se organiza por fecha de procesamiento e identificador de lote. Primero se guarda `markets.json`, con el texto original, y después `manifest.json`, con parámetros, fechas, modo, filas y bytes UTF-8. `put(..., False)` impide sobrescribir un archivo ya existente. El manifiesto enlaza el archivo con su lote; no contiene una clave API. Un replay conserva la hora de extracción original y registra por separado la hora de procesamiento.

# CELL ********************

# Se guarda primero la respuesta original, antes de escribir cualquier tabla Delta.
base = f"Files/bronze/coingecko/fecha={started[:10]}/batch_id={batch_id}"
notebookutils.fs.mkdirs(base)
notebookutils.fs.put(base+"/markets.json", payload, False)
manifest = {"batch_id":batch_id,"url":url,"parameters":params,"extracted_at_utc":extracted,
            "processed_at_utc":started,"mode":mode,"replay_source":replay_path,"rows":len(data),
            "bytes_utf8":len(payload.encode("utf-8")),"raw_path":base+"/markets.json"}
notebookutils.fs.put(base+"/manifest.json",json.dumps(manifest,ensure_ascii=False,indent=2),False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Tabla Bronze con append
# 
# `CREATE SCHEMA IF NOT EXISTS` prepara el espacio de tablas, no una carpeta de Files. Cada fila conserva lote, hora de ingesta, hora de extracción, ruta al JSON, posición de la fila y el registro serializado. El esquema explícito evita que Spark tenga que inferir tipos a partir de la muestra. `append` añade el lote a `bronze.mercado_raw`; nunca reemplaza los anteriores. `record_json` conserva los atributos de origen sin aplicar reglas de negocio.

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
rows = [(batch_id, started, extracted, base+"/markets.json", i,
         json.dumps(row,ensure_ascii=False,separators=(",",":"))) for i,row in enumerate(data)]
bronze_schema = "batch_id string, ingestado_utc string, extraido_utc string, ruta_json string, registro_num long, record_json string"
raw = spark.createDataFrame(rows, bronze_schema)
raw.write.format("delta").mode("append").saveAsTable("bronze.mercado_raw")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Categorías de CoinGecko: qué activos pertenecen a cada categoría (dato crudo).
CATEGORIAS_API = ["stablecoins", "tokenized-gold", "tokenized-treasuries", "tokenized-t-bills",
                  "tokenized-money-market-fund-mmfs", "tokenized-credit",
                  "tokenized-private-credit", "real-estate"]
cat_extraido = datetime.now(timezone.utc).isoformat()
cat_rows = []
for cat_id in CATEGORIAS_API:
    for attempt in range(3):
        r = requests.get(url, params={"vs_currency":"usd","category":cat_id,"order":"market_cap_desc",
                                      "per_page":250,"page":1,"sparkline":"false"},
                         timeout=(15,60), headers={"Accept":"application/json"})
        if r.status_code not in (429,500,502,503,504):
            break
        if attempt < 2:
            time.sleep(min(int(r.headers.get("Retry-After", 5 * (2**attempt))),30))
    r.raise_for_status()
    cat_rows += [(batch_id, cat_extraido, cat_id, x["id"]) for x in r.json()]
    time.sleep(3)  # respetar el límite de la API pública
if not any(c == "stablecoins" for _, _, c, _ in cat_rows):
    raise ValueError("La API no devolvió la categoría stablecoins; no se publica el lote.")
cat_schema = "batch_id string, extraido_utc string, category_id string, coin_id string"
spark.createDataFrame(cat_rows, cat_schema).write.format("delta").mode("append").saveAsTable("bronze.categoria_raw")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Comprobación y entrega a Silver
# 
# Se vuelve a contar el lote escrito en Delta y se compara con la respuesta original. Solo si coincide se actualiza `current_batch.json`, el puntero que leerá Silver. Aquí `put(..., True)` es intencionado: el puntero representa el lote actual. No autoriza aún publicar Gold. El resultado impreso permite localizar el lote y su respuesta original.

# CELL ********************

count = spark.table("bronze.mercado_raw").filter(F.col("batch_id")==batch_id).count()
if count != len(data):
    raise ValueError(f"Bronze incompleto: {count} de {len(data)} registros")
notebookutils.fs.mkdirs("Files/bronze/_control/coingecko")
notebookutils.fs.put("Files/bronze/_control/coingecko/current_batch.json",json.dumps(manifest,ensure_ascii=False),True)
print(json.dumps({"fase":"Bronze","batch_id":batch_id,"filas":count,"raw_path":manifest["raw_path"]},ensure_ascii=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
