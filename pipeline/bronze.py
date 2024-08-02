# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

checkpoint_volume = f"/Volumes/{catalog}/{schema}/checkpoints"
schema_volume = f"/Volumes/{catalog}/{schema}/schemas"
location = "abfss://dw-landing@oneenvadls.dfs.core.windows.net"
folders = [
    # ("productobs", ";"),
    ("instituciones", ","),
    ("clasificacionbs", ","),
    ("adjudicacionesproveedores", ","),
    ("proveedores", ";"),
    ("solicitudes", ","),
    ("lineasadjudicadas", ","),
    ("ofertas", ","),
    ("jerarquiaClasificacionBS", ","),
    ("lineasofertadas", ","),
    ("lineasproc", ","),
    ("consorcios", ","),
    ("adjudicaciones", ","),
    ("funcionarioRol", ","),
    ("identificacionbs", ","),
    ("aclaraciones", ","),
    ("sector_institucional", ";"),
    ("representantes", ","),
    ("procedimientos", ","),
]

# COMMAND ----------

for file in folders:
    print(file)
    try:
        name = file[0]
        file_delimiter = file[1]
        q = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("multiline", "true")
            .option("escape", '"')
            .option("sep", file_delimiter)
            .option("cloudFiles.schemaLocation", f"{schema_volume}/schema_{name}")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"{location}/{name}")
            .writeStream.format("delta")
            .option("checkpointLocation", f"{checkpoint_volume}/checkpoint_{name}")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .table(f"`{catalog}`.{schema}.{name}")
        )
        q.awaitTermination()
    except Exception as e:
        print(e)
        raise e
