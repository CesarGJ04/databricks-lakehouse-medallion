# Databricks notebook source
from pyspark.sql.functions import to_date, lit
from pyspark.sql.functions import date_format, add_months
from delta.tables import DeltaTable
from datetime import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

dbutils.widgets.text( "tipo_carga",  "Incremental", "tipo_carga")

# COMMAND ----------

tipo_carga = dbutils.widgets.get("tipo_carga")

# COMMAND ----------

dbutils.widgets.text( "fecha_carga",  "2025-06-16", "fecha_carga")

# COMMAND ----------

fecha_carga_str = dbutils.widgets.get("fecha_carga")
fecha_carga = to_date(lit(fecha_carga_str),"yyyy-MM-dd")

periodo_corte = datetime.strptime(fecha_carga_str, '%Y-%m-%d').date().replace(day=1) - relativedelta(months=2)
periodo_corte = periodo_corte.strftime('%Y%m')

# COMMAND ----------

periodo_corte

# COMMAND ----------

path_bronze_compras = "workspace.linio.bronze_compras"
path_bronze_detalles = "workspace.linio.bronze_detalles"

# COMMAND ----------

from pyspark.sql.functions import col, when, trim, upper, initcap, lpad, to_date, lit, length, split, concat, concat_ws, datediff, regexp_replace, regexp_extract, round, current_timestamp, datediff

# COMMAND ----------

# Importar base COMPRAS desde la tabla Delta en la capa Bronze
df_compras = (
    spark.table(path_bronze_compras)
    .select(
        'venta_id',
        'factura',
        'fecha_orden',
        'fecha_entrega',
        'fecha_envio',            
        'estado',
        'cliente_code',
        'tipo_cliente',
        'nombres',
        'apellidos',
        'vendedor',
        'departamento',
        'metodo_pago',
        'tipo_compra'
           )
    )
df_compras = (
    df_compras
    .withColumn("venta_id", col("venta_id").cast("integer"))
    .withColumn("estado", col("estado").cast("integer"))
    .withColumn("fecha_orden", col("fecha_orden").cast("date"))
    .withColumn("fecha_entrega",to_date(col("fecha_entrega"), "dd/MM/yyyy"))
    .withColumn("fecha_envio",to_date(col("fecha_envio"), "dd-MM-yy"))
)


# COMMAND ----------

df_compras.limit(10).display()

# COMMAND ----------

df_compras = (
    df_compras
        # Normalización de strings
        .withColumn("factura", upper(trim(col("factura"))))
        .withColumn("tipo_cliente", upper(trim(col("tipo_cliente"))))
        .withColumn("nombres", initcap(trim(col("nombres"))))
        .withColumn("apellidos", initcap(trim(col("apellidos"))))
        .withColumn("vendedor", trim(col("vendedor")))
        .withColumn("departamento", trim(col("departamento")))
        .withColumn("metodo_pago", trim(col("metodo_pago")))

        # Mapear estado numérico a texto
        .withColumn(
            "estado",
            when(col("estado") == 1, "Creado")
            .when(col("estado") == 2, "En Curso")
            .when(col("estado") == 3, "Programado")
            .when(col("estado") == 4, "Cancelado")
            .when(col("estado") == 5, "Entregado")
            .otherwise("No Definido")
        )

        # Parsear cliente_code
        .withColumn("cliente_id", split(col("cliente_code"), "-").getItem(0).cast("integer"))
        .withColumn("num_documento", split(col("cliente_code"), "-").getItem(1).cast("string"))

        # Completar con ceros a la izquierda si < 8
        .withColumn(
            "num_documento",
            when(length(col("num_documento")) < 8, lpad(col("num_documento"), 8, "0"))
            .otherwise(col("num_documento"))
        )

        # Definir tipo_documento
        .withColumn(
            "tipo_documento",
            when((length(col("num_documento")) == 8), lit("DNI"))
            .when((length(col("num_documento")) == 11) & (col("num_documento").startswith("10")), lit("RUC10"))
            .when((length(col("num_documento")) == 11) & (col("num_documento").startswith("20")), lit("RUC20"))
            .otherwise(lit("NULL"))
        )

        # Concatenar nombre completo
        .withColumn("nombre_cliente", concat(col("nombres"), lit(" "), col("apellidos")))

        # Calcular dias abierto
        .withColumn(
              "dias_abierto",
              when(col("estado").isin("Creado","En curso","Programado"), datediff(lit(fecha_carga), col("fecha_orden")))
              .otherwise(lit(None))
              )

        # Calcular grupo deias abierto
        .withColumn(
            "grupo_dias_abierto",
            when(col("dias_abierto").isNull(), lit(None))
            .when(col("dias_abierto") <= 3, "0-3 dias")
            .when(col("dias_abierto") <= 7, "4-7 dias")
            .otherwise("más de 8 dias"))
        .withColumn("periodo", date_format("fecha_orden", "yyyyMM"))
)

# COMMAND ----------

#df_compras = df_compras.withColumn("periodo", date_format("fecha_orden", "yyyyMM"))

# COMMAND ----------

df_compras.columns

# COMMAND ----------

df_compras = df_compras.select(
 'periodo',
 'venta_id',
 'factura',
 'tipo_compra',
 'fecha_orden',
 'fecha_entrega',
 'fecha_envio',
 'estado',
 'cliente_id',
 'tipo_documento',
 'num_documento',
 'nombre_cliente',
 'tipo_cliente',
 'vendedor',
 'departamento',
 'metodo_pago',
 'dias_abierto',
 'grupo_dias_abierto'
).withColumn("fecha_carga", fecha_carga)

# COMMAND ----------

df_compras.limit(10).display()

# COMMAND ----------

df_detalles = (
    spark.table(path_bronze_detalles)
    .select(
        'detalle_id',
        'factura',
        'oferta_id',
        'categoria',
        'subcategoria',
        'producto',
        'unidades',
        'precio_unitario',
        'nombre_archivo'
           ))

# COMMAND ----------

df_detalles.limit(10).display()

# COMMAND ----------

df_detalles = (
    df_detalles
    # Cambio de tipo de datos
    .withColumn("detalle_id", col("detalle_id").cast("integer"))
    .withColumn("unidades", col("unidades").cast("integer"))
    .withColumn("oferta_id", col("oferta_id").cast("integer"))
    .withColumn("precio_unitario", col("precio_unitario").cast("double"))

    # Limpieza de datos
    .withColumn("factura", upper(trim(col("factura"))))
    .withColumn("categoria", trim(col("categoria")))
    .withColumn("subcategoria", trim(col("subcategoria")))
    .withColumn("producto", trim(col("producto")))

    # Transformaciones de datos
    .withColumn("subtotal", col("unidades") * col("precio_unitario"))
    .withColumn("tienda", split(col("nombre_archivo"), r"\.").getItem(0))
    .withColumn("fecha_carga", current_timestamp())

    # Selección final de columnas
    .select(
        "detalle_id",
        "factura",
        "oferta_id",
        "categoria",
        "subcategoria",
        "producto",
        "unidades",
        "subtotal",
        "tienda",
        "fecha_carga"
    )
)

# COMMAND ----------

df_detalles.limit(10).display()

# COMMAND ----------

df_detalles.columns

# COMMAND ----------

df_detalles = df_detalles.select(
    'detalle_id',
    'factura',
    'tienda',
    'oferta_id',
    'categoria',
    'subcategoria',
    'producto',
    'unidades',
    'subtotal',
    'fecha_carga'
)

# COMMAND ----------

#spark.sql("DROP TABLE IF EXISTS linio.silver_compras")

# COMMAND ----------

try:
    # Carga de datos de la base COMPRAS a la tabla Delta en la capa Silver
    if tipo_carga == "Historico":        
        (
            df_compras.write
                    .format("delta")
                    .mode("overwrite")
                    .partitionBy("periodo")
                    .option("overwriteSchema", "true")
                    .saveAsTable("workspace.linio.silver_compras")
        )
        # Carga de datos de la base DETALLES a la tabla Delta en la capa Silver
        (
            df_detalles.write
                    .format("delta")
                    .mode("overwrite")
                    .saveAsTable("workspace.linio.silver_detalles")
        )

        spark.sql('''
                  OPTIMIZE workspace.linio.silver_detalles
                  ZORDER BY (detalle_id)
                  ''')
        
    elif tipo_carga == "Incremental":
        df_compras = df_compras.filter(col("periodo") >= lit(periodo_corte))
        (
            df_compras.write
                    .format("delta")
                    .mode("append")
                    .option("replaceWhere", "periodo >= {}".format(periodo_corte))
                    .saveAsTable("workspace.linio.silver_compras")
        )
        dbutils.jobs.taskValues.set(key="estado", value="OK") 

        #Carga de dataos de la base DETALLES a la tabla Delta en la capa Silver
        tabla_delta = DeltaTable.forName(spark, "workspace.linio.silver_detalles")
        
        (
            tabla_delta.alias("t")
                        .merge(df_detalles.alias("n"), "t.detalle_id = n.detalle_id")
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
        )
        dbutils.jobs.taskValues.set(key="estado", value="OK" ) 
    else:
        dbutils.jobs.taskValues.set(key="estado", value="No Procesado" ) 

except Exception as e:
    import traceback
    error_type   =  type(e).__name__
    error_summary =  str(e)
    error_trace  =  traceback.format_exc()

    error_msg_full = f"{error_type}: {error_summary}\n{error_trace}"
    
    if len(error_msg_full) > 500:
        error_msg = error_msg_full[:500] + "\n[...] ERROR TRUNCADO [...]"
    
    else:
        error_msg = error_msg_full
    
    dbutils.jobs.taskValues.set(key="error", value=error_msg)
    raise e