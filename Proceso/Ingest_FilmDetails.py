# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS catalog_dev.bronze.filmdetails;

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

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/FilmDetails.csv"

# COMMAND ----------

filmdetails_schema = StructType(fields=[StructField("id", IntegerType(), False),
                                  StructField("director", StringType(), True),
                                  StructField("top_billed", StringType(), True),
                                  StructField("budget_usd", DoubleType(), True),
                                  StructField("revenue_usd", DoubleType(), True) 
])

# COMMAND ----------

filmdetails_df = spark.read \
            .option("header", True) \
            .schema(filmdetails_schema) \
            .csv(ruta)

# COMMAND ----------

filmdetails_df_with_timestamp_df = filmdetails_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

filmdetails_selected_df = filmdetails_df_with_timestamp_df.select(col('id').alias('id_pelicula'), 
                                                                col('director'),col('top_billed').alias('actores'), 
                                                                col('budget_usd').alias('presupuesto_usd'), 
                                                                col('revenue_usd').alias('ingresos_usd'))

# COMMAND ----------

filmdetails_selected_df.write.mode('overwrite').saveAsTable(f'{catalogo}.{esquema}.filmdetails')


# COMMAND ----------

filmdetails_selected_df.show()
