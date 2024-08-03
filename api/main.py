import os 
import json
import numpy as np
from decimal import *
from fastapi import FastAPI
from databricks import sql

app = FastAPI()

connection = sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

CATALOG = "dw_dev"
SCHEMA = "gold"
LINEAS_ADJ = "proceso_contratacion"

def convert_to_serializable(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.generic):
        return obj.item()
    elif isinstance(obj, Decimal):
        return float(obj)
    return obj


def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    rows     = cursor.fetchall()

    column_names = [description[0] for description in cursor.description]
    result_dicts = [dict(zip(column_names, [convert_to_serializable(value) for value in row])) for row in rows]

    cursor.close()
    return result_dicts

@app.get("/")
async def root():
    return {"message": "Hello Data Challenge"}

@app.get("/consultar_sicop/{nro_sicop}")
async def get_by_nro_sicop(nro_sicop: str):
    query = f"SELECT * FROM {CATALOG}.{SCHEMA}.{LINEAS_ADJ} WHERE nro_sicop = {nro_sicop}"
    result_dicts = execute_query(connection, query)
    return {"data": result_dicts}

@app.get("/consultar")
async def get_by_nro_sicop(nro_sicop: str):
    query = "SELECT * FROM dw_dev.gold.proceso_contratacion"
    result_dicts = execute_query(connection, query)
    return {"data": result_dicts}