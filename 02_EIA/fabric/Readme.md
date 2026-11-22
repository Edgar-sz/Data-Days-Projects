# fabric/

Elementos del espacio de trabajo `EIA_gov` de Microsoft Fabric, sincronizados con este repositorio mediante la **integración Git de Fabric**.

| Carpeta | Contenido |
|---|---|
| `00_Data/` | Lakehouse `LH_EIA` (definición; los datos no se versionan) |
| `01_Pipeline/` | Notebooks `NB01_EIA_Silver` y `NB02_EIA_Gold`, y el pipeline `PL_EIA` |
| `02_Model/` | Modelo semántico `PBI_EIA` (formato TMDL) |
| `03_Report/` | Informe `PBI_EIA` (formato PBIR), con sus iconos |

## Qué debes saber
- **Esta carpeta la escribe Fabric.** Los cambios se hacen en Fabric y se confirman desde *Control de código fuente*; no la edites a mano.
- **Los notebooks están en formato `notebook-content.py`**, el formato de Fabric para Git. No se pueden importar tal cual como notebook con celdas. Para importarlos, usa los archivos `.ipynb` de [`../notebooks/`](../notebooks/).
- **La capa Bronze no tiene notebook:** la escribe la actividad Copy del pipeline `PL_EIA`.
- **La clave de la API no está aquí.** El pipeline la lee de `Files/config/eia_config.json` en el Lakehouse.
- **No incluye datos ni salidas de celdas.** Solo definiciones.
- Los identificadores del espacio de trabajo, las conexiones y la dirección del punto de conexión SQL son los del autor. Para replicar el proyecto, sigue los pasos del [README del proyecto](../README.md).
