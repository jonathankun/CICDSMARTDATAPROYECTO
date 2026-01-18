# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS catalog_dev.bronze.movies;

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

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/Movies.csv"

# COMMAND ----------

df_movies = spark.read.option('header', True)\
                        .option('inferSchema', True)\
                        .csv(ruta)

# COMMAND ----------

# DBTITLE 1,Untitled
movies_schema = StructType(fields=[StructField("id", IntegerType(), False),
                                     StructField("title", StringType(), True),
                                     StructField("genres", StringType(), True),
                                     StructField("language", StringType(), True),
                                     StructField("user_score", DoubleType(), True),
                                     StructField("runtime_hour", IntegerType(), True),
                                     StructField("runtime_min", IntegerType(), True),
                                     StructField("release_date", DateType(), True),
                                     StructField("vote_count", IntegerType(), True)
])

# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_movies_final = spark.read\
.option('header', True)\
.schema(movies_schema)\
.csv(ruta)


# COMMAND ----------

# DBTITLE 1,select only specific cols
movies_selected_df = df_movies_final.select(col("id"), 
                                                col("title"), 
                                                col("genres"), 
                                                col("language"), 
                                                col("user_score"), 
                                                col("runtime_hour"),
                                                col("runtime_min"),
                                                col("release_date"), 
                                                col("vote_count"))


# COMMAND ----------

movies_renamed_df = movies_selected_df.withColumnRenamed("id", "id_pelicula") \
                                        .withColumnRenamed("title", "titulo") \
                                        .withColumnRenamed("genres", "genero") \
                                        .withColumnRenamed("language", "idioma") \
                                        .withColumnRenamed("user_score", "record_usuario") \
                                        .withColumnRenamed("runtime_hour", "duracion_hora") \
                                        .withColumnRenamed("runtime_min", "duracion_minuto") \
                                        .withColumnRenamed("release_date", "fecha_lanzamiento") \
                                        .withColumnRenamed("vote_count", "votos")


# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
movies_final_df = movies_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

movies_final_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.movies")

# COMMAND ----------

movies_final_df.show()
