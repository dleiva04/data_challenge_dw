# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dw_dev.silver.lineas_adjudicadas AS
# MAGIC SELECT 
# MAGIC     CAST(A.nro_sicop AS STRING) AS nro_sicop,
# MAGIC     collect_set(
# MAGIC       struct(
# MAGIC         CAST(LA.nro_sicop AS STRING) AS nro_sicop,
# MAGIC         CAST(LA.nro_acto AS STRING) AS nro_acto,
# MAGIC         CAST(LA.nro_oferta AS STRING) AS nro_oferta,
# MAGIC         CAST(LA.nro_linea AS STRING) AS nro_linea,
# MAGIC         CAST(LA.codigo_producto AS STRING) AS codigo_producto,
# MAGIC         CAST(LA.cedula_proveedor AS STRING) AS cedula_proveedor,
# MAGIC         LA.cantidad_adjudicada,
# MAGIC         LA.precio_unitario_adjudicado,
# MAGIC         LA.tipo_moneda,
# MAGIC         LA.descuento,
# MAGIC         LA.iva,
# MAGIC         LA.otros_impuestos,
# MAGIC         LA.acarreos,
# MAGIC         LA.tipo_cambio_crc,
# MAGIC         LA.tipo_cambio_dolar
# MAGIC     )) AS lineas_adjudicadas,
# MAGIC     collect_set(
# MAGIC       struct(
# MAGIC         CAST(O.nro_oferta AS STRING) AS nro_oferta,
# MAGIC         CAST(O.cedula_proveedor AS STRING) AS cedula_proveedor,
# MAGIC         O.fecha_presenta_oferta,
# MAGIC         O.tipo_oferta,
# MAGIC         CAST(O.nro_consorcio AS STRING) AS nro_consorcio,
# MAGIC         O.elegible,
# MAGIC         O.estado
# MAGIC     )) AS ofertas,
# MAGIC
# MAGIC     collect_set(
# MAGIC       struct(
# MAGIC         CAST(P.cedula_proveedor AS STRING) AS cedula_proveedor,
# MAGIC         P.nombre_proveedor,
# MAGIC         P.tipo_proveedor,
# MAGIC         P.tamano_proveedor,
# MAGIC         P.fecha_constitucion,
# MAGIC         P.fecha_expiracion,
# MAGIC         P.direccion,
# MAGIC         P.codigo_postal,
# MAGIC         P.provincia,
# MAGIC         P.canton,
# MAGIC         P.distrito
# MAGIC     )) AS proveedores,
# MAGIC
# MAGIC     collect_set(
# MAGIC       struct(
# MAGIC         CAST(R.cedula_representante AS STRING) AS cedula_representante,
# MAGIC         R.nombre_representante,
# MAGIC         CAST(R.cedula_proveedor AS STRING) AS cedula_proveedor,
# MAGIC         R.tipo_representante,
# MAGIC         R.fecha_inscripcion,
# MAGIC         R.fecha_fin_presentacion
# MAGIC     )) AS representantes,
# MAGIC
# MAGIC     collect_set(
# MAGIC       struct(
# MAGIC         CAST(C.nro_consorcio AS STRING) AS nro_consorcio,
# MAGIC         CAST(C.cedula_proveedor AS STRING) AS cedula_proveedor
# MAGIC     )) AS consorcios
# MAGIC FROM dw_dev.bronze.adjudicaciones A 
# MAGIC INNER JOIN dw_dev.bronze.lineasadjudicadas LA
# MAGIC     ON A.nro_sicop = LA.nro_sicop AND A.nro_acto = LA.nro_acto
# MAGIC LEFT JOIN dw_dev.bronze.ofertas O
# MAGIC     ON LA.nro_sicop = O.nro_sicop AND LA.nro_oferta = O.nro_oferta
# MAGIC LEFT JOIN dw_dev.bronze.proveedores P
# MAGIC     ON LA.cedula_proveedor = P.cedula_proveedor
# MAGIC LEFT JOIN dw_dev.bronze.consorcios C
# MAGIC     ON P.cedula_proveedor = C.nro_consorcio
# MAGIC LEFT JOIN dw_dev.bronze.representantes R
# MAGIC     ON P.cedula_proveedor = R.cedula_proveedor
# MAGIC GROUP BY 
# MAGIC     A.nro_sicop;
