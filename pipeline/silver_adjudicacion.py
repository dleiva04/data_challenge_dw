# Databricks notebook source
spark.sql(
    """
CREATE OR REPLACE TABLE demo_datachallenge.silver.lineas_adjudicadas AS
SELECT 
    cast(a.nro_sicop as string),
    cast(a.nro_acto as string),
    cast(a.fecha_adj_firme as timestamp),
    cast(a.fecha_comunicacion as timestamp),
    case a.permite_recursos
        when 'Si' then true
        else false
    end as permite_recursos,
    case a.desierto
        when 'Y' then true
        else false
    end as desierto,
    case a.es_valido
        when 'Y' then true
        else false
    end as es_valido,
    cast(a.`version` as string),
    cast(LA.nro_oferta as string),
    cast(LA.nro_linea as string),
    cast(LA.cedula_proveedor as string),
    cast(LA.cantidad_adjudicada as string),
    cast(LA.precio_unitario_adjudicado as double),
    cast(LA.tipo_moneda as string),
    cast(LA.descuento as double),
    cast(LA.iva as double),
    cast(LA.otros_impuestos as double),
    cast(LA.acarreos as double),
    cast(LA.tipo_cambio_crc as double),
    cast(LA.tipo_cambio_dolar as double),
    cast(pbs.codigo_producto as string)
FROM demo_datachallenge.bronze.lineasadjudicadas LA 
LEFT JOIN demo_datachallenge.bronze.adjudicaciones a
    ON A.nro_sicop = LA.nro_sicop AND A.nro_acto = LA.nro_acto
LEFT JOIN demo_datachallenge.bronze.adjudicacionesproveedores ap
    ON LA.nro_acto = ap.nro_acto
LEFT JOIN demo_datachallenge.bronze.productobs pbs
    ON LA.codigo_producto = pbs.codigo_producto
GROUP BY all
          """
)
