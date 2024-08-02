# Databricks notebook source
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(
    f"""create or replace table {catalog}.{schema}.procedimientos_den as
select 
    p.nro_sicop,
    p.cedula_institucion,
    p.nro_procedimiento,
    p.cod_unidad_compra,
    p.nombre_unidad_compra,
    p.tipo_procedimiento,
    p.modalidad_procedimiento,
    p.excepcion_cd,
    p.descripcion,
    p.pago_adelantado_pymes,
    array_agg(
        named_struct(
            'nombre', fr.nombre_funcionario,
            'cedula', fr.cedula_funcionario,
            'rol', fr.rol
        )
    ) as funcionarios,
    si.nombre_institucion,
    si.Nota as nota,
    si.`Código` as codigo_hacienda,
    si.sector,
    p.fecha_publicacion,
    p.fecha_invitacion,
    p.fecha_inicio_recepcion,
    p.fecha_cierre_recepcion,
    p.fecha_apertura,
    p.fecha_limite_recursos
from {catalog}.{schema}.procedimientos p
left join {catalog}.{schema}.funcionarioRol fr
on p.nro_sicop = fr.nro_sicop
left join {catalog}.{schema}.sector_institucional si
on p.cedula_institucion = si.cedula
group by all;"""
)

# COMMAND ----------

spark.sql(
    f"""create or replace table {catalog}.{schema}.identificacion_den as
select 
    ibs.codigo_identificacion,
    ibs.nombre_identificacion,
    ibs.aspectos_ambientales_sociales,
    ibs.codigo_clasificacion,
    jbs.nombreClasificacion,
    jbs.codigoSegmento as codigo_segmento,
    jbs.nombreSegmento as nombre_segmento,
    jbs.codigoClase as codigo_clase,
    jbs.nombreClase as nombre_clase,
    jbs.codigoFamilia as codigo_familia,
    jbs.nombreFamilia as nombre_familia,
    array_agg(
        named_struct(
            'codigo', pbs.codigo_producto,
            'desc_bien_servicio', pbs.desc_bien_servicio
        )
    ) as productos
from {catalog}.{schema}.identificacionbs ibs
left join {catalog}.{schema}.jerarquiaClasificacionBS jbs
on ibs.codigo_clasificacion = jbs.codigoClasificacion
left join {catalog}.{schema}.productobs pbs
on ibs.codigo_identificacion = pbs.codigo_identificacion
group by all;
          """
)

# COMMAND ----------

spark.sql(
    f"""create or replace table {catalog}.silver_test.lineasproc as
select 
    lp.nro_sicop,
    lp.numero_linea,
    lp.numero_partida,
    lp.cantidad_solicitada,
    lp.precio_unitario_estimado,
    lp.tipo_moneda,
    lp.tipo_cambio_crc,
    lp.tipo_cambio_dolar,
    lp.codigo_identificacion,
    lp.subpartida_og,
    pden.cedula_institucion,
    pden.nro_procedimiento,
    pden.cod_unidad_compra,
    pden.nombre_unidad_compra,
    pden.tipo_procedimiento,
    pden.modalidad_procedimiento,
    pden.excepcion_cd,
    pden.descripcion,
    pden.pago_adelantado_pymes,
    pden.funcionarios,
    pden.nombre_institucion,
    pden.nota,
    pden.codigo_hacienda,
    pden.sector,
    pden.fecha_publicacion,
    pden.fecha_invitacion,
    pden.fecha_inicio_recepcion,
    pden.fecha_cierre_recepcion,
    pden.fecha_apertura,
    pden.fecha_limite_recursos,
    iden.nombre_identificacion,
    iden.aspectos_ambientales_sociales,
    iden.codigo_clasificacion,
    iden.nombreClasificacion,
    iden.codigo_segmento,
    iden.nombre_segmento,
    iden.codigo_clase,
    iden.nombre_clase,
    iden.codigo_familia,
    iden.nombre_familia,
    iden.productos
from {catalog}.{schema}.lineasproc lp
left join {catalog}.{schema}.procedimientos_den pden
on lp.nro_sicop = pden.nro_sicop
left join {catalog}.{schema}.identificacion_den iden
on lp.codigo_identificacion = iden.codigo_identificacion;"""
)
