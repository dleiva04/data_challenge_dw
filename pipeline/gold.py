# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table dw_dev.gold.proceso_contratacion as
# MAGIC select 
# MAGIC     lplo.nro_sicop,
# MAGIC     lplo.nro_linea,
# MAGIC     lplo.ofertas,
# MAGIC     lpla.adjudicadas
# MAGIC from (
# MAGIC     select 
# MAGIC         lp.nro_sicop,
# MAGIC         lo.nro_linea,
# MAGIC         array_agg(lo.nro_oferta) as ofertas
# MAGIC     from dw_dev.silver.lineasproc lp
# MAGIC     left join dw_dev.silver.lineasofertadas lo
# MAGIC     on lp.nro_sicop = lo.nro_sicop
# MAGIC     and lp.numero_linea = lo.nro_linea
# MAGIC     group by lp.nro_sicop, lo.nro_linea
# MAGIC ) lplo 
# MAGIC left join (
# MAGIC     select 
# MAGIC         lp.nro_sicop,
# MAGIC         la.nro_linea,
# MAGIC         array_agg(la.nro_oferta) as adjudicadas
# MAGIC     from dw_dev.silver.lineasproc lp
# MAGIC     left join dw_dev.silver.lineas_adjudicadas la
# MAGIC     on lp.nro_sicop = la.nro_sicop
# MAGIC     and lp.numero_linea = la.nro_linea
# MAGIC     group by lp.nro_sicop, la.nro_linea
# MAGIC ) lpla
# MAGIC on lplo.nro_sicop = lpla.nro_sicop
# MAGIC and lplo.nro_linea = lpla.nro_linea
# MAGIC where lplo.nro_linea is not null;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dw_dev.gold.sumatorias as
# MAGIC select * from (
# MAGIC select
# MAGIC     "adjudicacion" as tipo,
# MAGIC     nro_sicop,
# MAGIC     nro_linea,
# MAGIC     nro_oferta,
# MAGIC     tipo_moneda,
# MAGIC     if(
# MAGIC         tipo_moneda='CRC', 
# MAGIC         (precio_unitario_adjudicado * cantidad_adjudicada - descuento + iva + otros_impuestos + acarreos)/tipo_cambio_dolar,
# MAGIC         precio_unitario_adjudicado * cantidad_adjudicada - descuento + iva + otros_impuestos + acarreos
# MAGIC     ) as total_dolar,
# MAGIC     if(
# MAGIC         tipo_moneda='USD', 
# MAGIC         (precio_unitario_adjudicado * cantidad_adjudicada - descuento + iva + otros_impuestos + acarreos)*tipo_cambio_crc,
# MAGIC         precio_unitario_adjudicado * cantidad_adjudicada - descuento + iva + otros_impuestos + acarreos
# MAGIC     ) as total_crc,
# MAGIC     cantidad_adjudicada,
# MAGIC     precio_unitario_adjudicado,
# MAGIC     descuento,
# MAGIC     iva,
# MAGIC     otros_impuestos,
# MAGIC     acarreos
# MAGIC from dw_dev.silver.lineas_adjudicadas
# MAGIC union all
# MAGIC select
# MAGIC     "oferta" as tipo,
# MAGIC     nro_sicop,
# MAGIC     nro_linea,
# MAGIC     nro_oferta,
# MAGIC     tipo_moneda,
# MAGIC     if(
# MAGIC         tipo_moneda='CRC', 
# MAGIC         (precio_unitario_ofertado * cantidad_ofertada - descuento + iva + otros_impuestos + acarreos)/tipo_cambio_dolar,
# MAGIC         precio_unitario_ofertado * cantidad_ofertada - descuento + iva + otros_impuestos + acarreos
# MAGIC     ) as total_dolar,
# MAGIC     if(
# MAGIC         tipo_moneda='USD', 
# MAGIC         (precio_unitario_ofertado * cantidad_ofertada - descuento + iva + otros_impuestos + acarreos)*tipo_cambio_crc,
# MAGIC         precio_unitario_ofertado * cantidad_ofertada - descuento + iva + otros_impuestos + acarreos
# MAGIC     ) as total_crc,
# MAGIC     cantidad_ofertada,
# MAGIC     precio_unitario_ofertado,
# MAGIC     descuento,
# MAGIC     iva,
# MAGIC     otros_impuestos,
# MAGIC     acarreos
# MAGIC from dw_dev.silver.lineasofertadas
# MAGIC );
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace table dw_dev.gold.linea_adjudicadas_bi as
# MAGIC SELECT
# MAGIC lp.nro_sicop,
# MAGIC lp.numero_linea,
# MAGIC lp.numero_partida,
# MAGIC lp.cantidad_solicitada,
# MAGIC lp.precio_unitario_estimado,
# MAGIC lp.tipo_moneda,
# MAGIC lp.tipo_cambio_crc,
# MAGIC lp.tipo_cambio_dolar,
# MAGIC lp.codigo_identificacion,
# MAGIC lp.subpartida_og,
# MAGIC lp.cedula_institucion,
# MAGIC lp.nro_procedimiento,lp.cod_unidad_compra,
# MAGIC lp.nombre_unidad_compra,
# MAGIC lp.tipo_procedimiento,
# MAGIC lp.modalidad_procedimiento,
# MAGIC lp.excepcion_cd,
# MAGIC lp.descripcion,
# MAGIC la.nro_acto,
# MAGIC la.fecha_adj_firme,
# MAGIC la.fecha_comunicacion,
# MAGIC la.permite_recursos,
# MAGIC la.desierto,
# MAGIC la.es_valido,
# MAGIC la.nro_oferta,
# MAGIC la.cedula_proveedor,
# MAGIC la.cantidad_adjudicada,
# MAGIC la.precio_unitario_adjudicado,
# MAGIC la.tipo_moneda,
# MAGIC la.descuento,
# MAGIC la.iva,
# MAGIC la.otros_impuestos,
# MAGIC la.acarreos,
# MAGIC la.tipo_cambio_crc,
# MAGIC la.tipo_cambio_dolar,
# MAGIC la.codigo_producto
# MAGIC FROM dw_dev.silver.lineasproc  lp
# MAGIC INNER JOIN dw_dev.silver.lineas_adjudicadas AS la ON (lp.nro_sicop=la.nro_sicop) AND (lp.numero_linea=la.nro_linea)
# MAGIC

# COMMAND ----------


