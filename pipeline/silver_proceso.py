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
    f"""create or replace table {catalog}.silver.lineasproc as
select 
    cast(lp.nro_sicop as string),
    cast(lp.numero_linea as int) as nro_linea,
    cast(lp.numero_partida as int) as nro_partida,
    cast(lp.cantidad_solicitada as int),
    cast(lp.precio_unitario_estimado as double),
    cast(lp.tipo_moneda as string),
    cast(lp.tipo_cambio_crc as double),
    cast(lp.tipo_cambio_dolar as double),
    cast(lp.codigo_identificacion as string),
    cast(lp.subpartida_og as string),
    cast(pden.cedula_institucion as string),
    cast(pden.nro_procedimiento as string),
    cast(pden.cod_unidad_compra as string),
    cast(pden.nombre_unidad_compra as string),
    cast(pden.tipo_procedimiento as string),
    cast(pden.modalidad_procedimiento as string),
    cast(pden.excepcion_cd as string),
    cast(pden.descripcion as string),
    case pago_adelantado_pymes
        when 'Y' then true
        else false
    end as pago_adelantado_pymes,
    pden.funcionarios,
    cast(pden.nombre_institucion as string),
    cast(pden.nota as string),
    cast(pden.codigo_hacienda as string),
    cast(pden.sector as string),
    cast(pden.fecha_publicacion as timestamp),
    cast(pden.fecha_invitacion as timestamp),
    cast(pden.fecha_inicio_recepcion as timestamp),
    cast(pden.fecha_cierre_recepcion as timestamp),
    cast(pden.fecha_apertura as timestamp),
    cast(pden.fecha_limite_recursos as timestamp),
    cast(iden.nombre_identificacion as string),
    cast(iden.aspectos_ambientales_sociales as string),
    cast(iden.codigo_clasificacion as string),
    cast(iden.nombreClasificacion as string),
    cast(iden.codigo_segmento as string),
    cast(iden.nombre_segmento as string),
    cast(iden.codigo_clase as string),
    cast(iden.nombre_clase as string),
    cast(iden.codigo_familia as string),
    cast(iden.nombre_familia as string),
    iden.productos
from {catalog}.{schema}.lineasproc lp
left join {catalog}.{schema}.procedimientos_den pden
on lp.nro_sicop = pden.nro_sicop
left join {catalog}.{schema}.identificacion_den iden
on lp.codigo_identificacion = iden.codigo_identificacion;"""
)
