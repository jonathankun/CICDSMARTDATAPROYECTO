# Databricks notebook source
from pyspark.sql.functions import concat, col, lit

# COMMAND ----------

import getpass

# COMMAND ----------

jdbcHostname = "serversmproyectojjrr.database.windows.net"
jdbcDatabase = "dbsd_proyecto_jjrr"
jdbcPort = 1433
jdbcUsername = "smartdata"
jdbcPassword = getpass.getpass("Introduce la contraseña de la base de datos: ")

# COMMAND ----------

# URL JDBC
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

df_golden = spark.sql("""select * from catalog_dev.golden.golden_movies_partitioned """)

# COMMAND ----------

df_golden.write \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "golden_movies_partitioned") \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .mode("overwrite") \
    .save()
