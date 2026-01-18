# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS catalog_dev.bronze.posterpath;

# COMMAND ----------

dbutils.widgets.text("storage_name", "adlsproyectojjrr")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

storage_name = dbutils.widgets.get("storage_name")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/PosterPath.csv"

# COMMAND ----------

posterpath_schema = StructType(fields=[StructField("id", IntegerType(), False),
                                  StructField("poster_path", StringType(), True),
                                  StructField("backdrop_path", StringType(), True) 
])

# COMMAND ----------

posterpath_df = spark.read \
            .option("header", True) \
            .schema(posterpath_schema) \
            .csv(ruta)

# COMMAND ----------

posterpath_df_with_timestamp_df = posterpath_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

posterpath_selected_df = posterpath_df_with_timestamp_df.select(col('id').alias('id_pelicula'), 
                                                                col('poster_path').alias('imagen_poster'), 
                                                                col('backdrop_path').alias('imagen_fondo'))

# COMMAND ----------

posterpath_selected_df.write.mode('overwrite').saveAsTable(f'{catalogo}.{esquema}.posterpath')


# COMMAND ----------

posterpath_selected_df.show()
