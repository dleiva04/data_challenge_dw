# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.functions import collect_list, to_json, struct,from_json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, when

# COMMAND ----------

# Reading data from the lineasofertadas table in the development catalog
lineas_ofertadas_df= spark.read.table("dw_dev.bronze.lineasofertadas")\
    .withColumn("nro_sicop", col("nro_sicop").cast("string")) \
    .withColumn("nro_linea", col("nro_linea").cast("string")) \
    .withColumn("codigo_producto", col("codigo_producto").cast("string"))

# COMMAND ----------

ofertas_df= spark.read.table("dw_dev.bronze.ofertas").distinct()


# COMMAND ----------

# Load the tables
proveedores_df = spark.table("dw_dev.bronze.proveedores").withColumn("cedula_proveedor", col("cedula_proveedor").cast("string"))

# COMMAND ----------

representantes_df = spark.table("dw_dev.bronze.representantes")\
    .withColumn("cedula_representante", col("cedula_representante").cast("string"))\
    .withColumn("cedula_proveedor", col("cedula_proveedor").cast("string"))

# COMMAND ----------

# Group by the relevant columns and collect the rest as JSON
representantes_df = representantes_df.groupBy("cedula_proveedor") \
    .agg(collect_list(struct("cedula_representante", "nombre_representante","tipo_representante","fecha_inscripcion","fecha_fin_presentacion")).alias("lista_de_representantes"))

# Convert the collected list to JSON format
representantes_df = representantes_df.withColumn("lista_de_representantes", to_json("lista_de_representantes"))

# COMMAND ----------

schema = ArrayType(StructType([
    StructField("cedula_representante", StringType(), True),
    StructField("nombre_representante", StringType(), True),
    StructField("tipo_representante", StringType(), True),
    StructField("fecha_inscripcion", TimestampType(), True),
    StructField("fecha_fin_presentacion", TimestampType(), True)
]))

# Assuming representantes_df is your existing DataFrame with JSON strings in lista_de_representantes column
representantes_df = representantes_df.withColumn("lista_de_representantes", from_json(col("lista_de_representantes"), schema))

# COMMAND ----------

consorcios_df= spark.table("dw_dev.bronze.consorcios")\
    .withColumn("nro_consorcio", col("nro_consorcio").cast("string"))\
    .withColumn("cedula_proveedor", col("cedula_proveedor").cast("string"))


# COMMAND ----------

# Group by 'cedula_proveedor' and collect 'nro_consorcio' values into a list
consorcios_proveedor_collapse_df = consorcios_df.groupBy("cedula_proveedor") \
    .agg(collect_list("nro_consorcio").alias("lista_de_consorcios_del_proveedor"))

# Convert the collected list to JSON format
consorcios_proveedor_collapse_df = consorcios_proveedor_collapse_df.withColumn(
    "lista_de_consorcios_del_proveedor", 
    to_json(col("lista_de_consorcios_del_proveedor"))
)



# COMMAND ----------

schema = ArrayType(StringType())

# Assuming consorcios_proveedor_collapse_df is your existing DataFrame with JSON strings in lista_de_consorcios_del_proveedor column
consorcios_proveedor_collapse_df = consorcios_proveedor_collapse_df.withColumn(
    "lista_de_consorcios_del_proveedor", 
    from_json(col("lista_de_consorcios_del_proveedor"), schema)
)

# COMMAND ----------

# Group by 'cedula_proveedor' and collect 'nro_consorcio' values into a list
consorcios_ofertas_collapse_df = consorcios_df.groupBy("nro_consorcio") \
    .agg(collect_list("cedula_proveedor").alias("lista_de_proveedores_del_consorcio"))

# Convert the collected list to JSON format
consorcios_ofertas_collapse_df = consorcios_ofertas_collapse_df.withColumn(
    "lista_de_proveedores_del_consorcio", 
    to_json(col("lista_de_proveedores_del_consorcio"))
)


# COMMAND ----------

schema = ArrayType(StringType())

consorcios_ofertas_collapse_df = consorcios_ofertas_collapse_df.withColumn(
    "lista_de_proveedores_del_consorcio", 
    from_json(col("lista_de_proveedores_del_consorcio"), schema))

# COMMAND ----------

proceso_ofertas_df=ofertas_df.join(lineas_ofertadas_df,on=["nro_oferta","nro_sicop"],how="left").join(proveedores_df,on="cedula_proveedor",how="left")

# COMMAND ----------

proceso_ofertas_df=ofertas_df.join(lineas_ofertadas_df,on=["nro_oferta","nro_sicop"],how="left").join(proveedores_df,on="cedula_proveedor",how="left")

# COMMAND ----------

proceso_ofertas_df=proceso_ofertas_df.join(representantes_df,on="cedula_proveedor",how="left")

# COMMAND ----------

proceso_ofertas_df=proceso_ofertas_df.join(consorcios_proveedor_collapse_df,on="cedula_proveedor",how="left")

# COMMAND ----------

proceso_ofertas_df = proceso_ofertas_df.withColumn(
    "estado",
    when(col("estado") == "Presentada", True)
    .when(col("estado") == "No Presentada", False)
    .otherwise(None)
    .cast("boolean")
)

# COMMAND ----------

proceso_ofertas_df = proceso_ofertas_df.withColumn(
    "elegible",
    when(col("elegible") == "Sí", True)
    .when(col("elegible") == "No", False)
    .otherwise(None)
    .cast("boolean")
)

# COMMAND ----------



proceso_ofertas_df = proceso_ofertas_df.withColumn("nro_sicop", col("nro_sicop").cast("string"))


# COMMAND ----------

proceso_ofertas_df = proceso_ofertas_df.withColumn("cedula_proveedor", col("cedula_proveedor").cast("string"))


# COMMAND ----------

proceso_ofertas_df = proceso_ofertas_df.withColumn("nro_consorcio", col("nro_consorcio").cast("string"))

# COMMAND ----------

# Eliminar las columnas de rescue values
columns_to_drop = [col for col in proceso_ofertas_df.columns if 'rescue' in col]
proceso_ofertas_df = proceso_ofertas_df.drop(*columns_to_drop)

# Mostrar una lista de las columnas
display(proceso_ofertas_df.count())

# COMMAND ----------

display(proceso_ofertas_df)

# COMMAND ----------

proceso_ofertas_df.write.mode("overwrite").saveAsTable("dw_dev.silver.lineasofertadas")

# COMMAND ----------


