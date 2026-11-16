# fabric/

Elementos del espacio de trabajo `Cripto_Snapshot_01` de Microsoft Fabric, sincronizados con este repositorio mediante la **integración Git de Fabric**.

| Carpeta | Contenido |
|---|---|
| `00_Data/` | Lakehouse `LH01_ObservatorioCripto` (definición; los datos no se versionan) |
| `01_Pipeline/` | Notebooks `NB01` a `NB03` y el pipeline `PL01_CoinGecko_Diario` |
| `02_Model/` | Modelo semántico `PBI_CoinGecko` (formato TMDL) |
| `03_Report/` | Informe `PBI_CoinGecko` (formato PBIR), con sus iconos |

## Qué debes saber
- **Esta carpeta la escribe Fabric.** Los cambios se hacen en Fabric y se confirman desde *Control de código fuente*; no la edites a mano.
- **Los notebooks están en formato `notebook-content.py`**, el formato de Fabric para Git. No se pueden importar tal cual como notebook con celdas. Para importarlos, usa los archivos `.ipynb` de [`../notebooks/`](../notebooks/).
- **No incluye datos ni salidas de celdas.** Solo definiciones.
- Los identificadores del espacio de trabajo y la dirección del punto de conexión SQL son los del autor. Para replicar el proyecto, sigue los pasos del [README del proyecto](../README.md).
