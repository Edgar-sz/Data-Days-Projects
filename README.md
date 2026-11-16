# Data Days Projects

Colección de proyectos de datos de punta a punta con **Microsoft Fabric** y **Power BI**, creada para la propuesta de Microsoft Data Days. Cada proyecto vive en su propia carpeta y se puede replicar en tu propio entorno: datos, pipeline, modelo semántico y tablero.

## Proyectos
| # | Proyecto | Qué hace | Tecnologías |
|---|---|---|---|
| 01 | [**Observatorio Cripto**](01_CoinGecko/) | Tablero de una página sobre el top 100 de criptoactivos: capitalización, volumen, dominancia de Bitcoin y calidad de los datos | Fabric (Lakehouse, notebooks, pipeline), Power BI, API de CoinGecko |

## Cómo está organizado cada proyecto
```
NN_Nombre/
├── README.md       Qué hace, arquitectura y cómo replicarlo
├── fabric/         Elementos de Fabric sincronizados con Git (Lakehouse, notebooks, pipeline, modelo, informe)
├── notebooks/      Los notebooks en formato .ipynb, listos para importar
├── powerbi/        El archivo .pbix, iconos y paleta de colores
└── docs/           Capturas y diagramas
```

La carpeta `fabric/` la escribe Fabric al sincronizar con Git; no se edita a mano.

## Requisitos generales
- Un espacio de trabajo de Microsoft Fabric con capacidad (sirve la prueba gratuita).
- Power BI Desktop, para abrir los archivos `.pbix`.
- Cada proyecto indica en su README los pasos concretos.
