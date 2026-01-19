# ANALISIS DE BD DE PELICULAS EN EL TIEMPO
---
**Proyecto de Ingeniería de Datos en Databricks - SmartData**
<hr>

## Descripción
Pipeline automatizado de datos para análisis de PELICULAS en el tiempo con arquitectura de tres capas y despliegue continuo.
<hr>


## Arquitectura
![](Arquitectura.png)
---
## 📁 Estructura del Proyecto

```
CICDSMARTDATAPROYECTO/
│
├── 📂 .github/
│   └── 📂 workflows/
│       └── 📄 deploy_dev_to_prod_databricks.yml   # Pipeline CI/CD deploy a certification workspace databricks
├── 📂 Proceso/
│   ├── Ingest_FilmDetails.py  # Bronze layer
│   ├── Ingest_PosterPath.py   # Bronze Layer
│   ├── Ingest_movies.py       # Bronze Layer
│   ├── Transform.py           # Silver Layer
│   └── Load.py                # Gold Layer
├── 📂 Scripts/
|   ├── Preparacion_Ambiente.py    # Create Schema, Tables, External location
|   └── Conexion-AzureSQL-Proj.py  # Conexión BD Azure SQL
├── 📂 Seguridad/
|   ├── Grants-Users-Groups.py  # Sql Grant
├── 📂 Reversion/
|   ├── Drop-revoke-proyecto.py   # Revoke permissions
├── 📂 dashboards/                    
|   ├── Dashboard_Movies.pdf           # Exportado PDF del Power BI
|   ├── Peliculas- Movies.pdf          # Exportado PDF del Dashboard Databricks
|   └── Peliculas- Movies.lvdash.json  #Exportado JSON del Dashboard Databricks
├── 📂 Datasets/ 
|   ├── FilmDetails.csv   # Fuente Dato 1
|   ├── Movies.csv        # Fuente Dato 2
|   └── PosterPath.csv    # Fuente Dato 3
└── 📄 README.md
```

### Capas del Pipeline

<table>
<tr>
<td width="33%" valign="top">

#### 🥉 Bronze Layer
**Propósito**: Zona de aterrizaje

**Tablas**: 
- `filmdetails` 
- `movies` 
- `posterpath`

**Características**:
- Datos tal como vienen de origen
- Timestamp de ingesta
- Preservación histórica
- Sin validaciones

</td>
<td width="33%" valign="top">

#### 🥈 Silver Layer
**Propósito**: Transformaciones

**Tablas**:
- `movies_transformed`

**Características**:
- Star Schema
- Datos normalizados
- Validaciones completas

</td>
<td width="33%" valign="top">

#### 🥇 Gold Layer
**Propósito**: Analytics-ready, agrupaciones

**Tablas**:
- golden_movies_partitioned : Peliculas agrupados por años en duración, ingresos, presupuesto, record, votos,etc


**Características**:
- Pre-agregados
- Optimizado para BI
- Performance máximo
- Actualizaciones automáticas

</td>
</tr>
</table>

## 🛠️ Tecnologías

<div align="center">

| Tecnología | Propósito |
|:----------:|:----------|
| ![Databricks](https://img.shields.io/badge/Azure_Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white) | Motor de procesamiento distribuido Spark |
| ![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat-square&logo=delta&logoColor=white) | Storage layer con ACID transactions |
| ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white) | Framework de transformación de datos |
| ![ADLS](https://img.shields.io/badge/ADLS_Gen2-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white) | Data Lake para almacenamiento persistente |
| ![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat-square&logo=github-actions&logoColor=white) | Automatización CI/CD |
| ![Databricks Dashboards](https://img.shields.io/badge/Databricks%20Dashboards-F2C81?style=for-the-badge&logo=databricks&logoColor=black) |  Visualización |
| ![Databricks Dashboards](https://img.shields.io/badge/Power_BI-Power_BI_Data_Analyst_Associate-FEB800) |  Visualización |

</div>

---
