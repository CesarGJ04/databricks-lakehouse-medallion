# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import to_date, lit
from pyspark.sql.functions import date_format, add_months
from delta.tables import DeltaTable
from datetime import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

dbutils.widgets.text( "tipo_carga",  "Incremental", "tipo_carga")
tipo_carga = dbutils.widgets.get("tipo_carga")

dbutils.widgets.text( "fecha_carga",  "2025-06-16", "fecha_carga")
fecha_carga_str = dbutils.widgets.get("fecha_carga")
fecha_carga = to_date(lit(fecha_carga_str),"yyyy-MM-dd")

periodo_corte = datetime.strptime(fecha_carga_str, '%Y-%m-%d').date().replace(day=1) - relativedelta(months=2)
periodo_corte = periodo_corte.strftime('%Y%m')

# COMMAND ----------

# Importar base COMPRAS desde la tabla Delta en la capa Silver
if tipo_carga == "Historico":
    df_compras = spark.table("workspace.linio.silver_compras")
elif tipo_carga == "Incremental":
    df_compras = spark.table("workspace.linio.silver_compras").filter(col("periodo") >= periodo_corte)
else:
    raise Exception("Tipo de carga no valido")


# COMMAND ----------


# Importar base DETALLES desde la tabla Delta en la capa Silver
df_detalles = spark.table("workspace.linio.silver_detalles")

# COMMAND ----------

# Combinar las bases COMPRAS y DETALLES
df_fact_compras = df_compras.join(df_detalles,"factura", "inner")

# COMMAND ----------

df_fact_compras =  df_fact_compras.select(
                       'venta_id',
                       'factura',
                       'fecha_orden',
                       'fecha_envio',
                       'estado',
                       'metodo_pago',
                       'grupo_dias_abierto',
                       'cliente_id',
                       'tipo_documento',
                       'num_documento',
                       'nombre_cliente',
                       'tipo_cliente',
                       'vendedor',
                       'departamento',
                       'detalle_id',
                       'tienda',
                       'categoria',
                       'subcategoria',
                       'producto',
                       'unidades',
                       'subtotal',
                       'periodo')
df_fact_compras = df_fact_compras.withColumn('fecha_carga', current_timestamp())

# COMMAND ----------

df_fact_compras.limit(10).display()

# COMMAND ----------

df_fact_compras.columns

# COMMAND ----------

df_fact_compras = df_fact_compras.select(
    'periodo',
    'factura',
    'venta_id',
    'fecha_orden',
    'fecha_envio',
    'estado',
    'metodo_pago',
    'grupo_dias_abierto',
    'cliente_id',
    'tipo_documento',
    'num_documento',
    'nombre_cliente',
    'tipo_cliente',
    'vendedor',
    'departamento',
    'detalle_id',
    'tienda',
    'categoria',
    'subcategoria',
    'producto',
    'unidades',
    'subtotal',
    'fecha_carga'
)

# COMMAND ----------

try:
    # Carga de datos de la base consolidada a la tabla Delta en la capa Gold
    if tipo_carga == "Historico":
        (
            df_fact_compras.write
                            .format("delta")
                            .mode("overwrite")
                            .partitionBy("periodo")
                            .option("overwriteSchema", True)
                            .saveAsTable("workspace.linio.gold_fact_compras")
        )
        dbutils.jobs.taskValues.set(key="estado", value="OK")

    elif tipo_carga == "Incremental":
        df_fact_compras = df_fact_compras.filter(col("periodo") >= periodo_corte)
        (
            df_fact_compras.write
                       .format("delta")
                       .mode("append")
                       .option("replaceWhere", f"periodo >= {periodo_corte}")
                       .saveAsTable("workspace.linio.gold_fact_compras")
        )
        dbutils.jobs.taskValues.set(key="estado", value="OK")
        
    else:
        dbutils.jobs.taskValues.set(key="error", value="Tipo de carga no valido")
        
except Exception as e:
    import traceback
    error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
    dbutils.jobs.taskValues.set(key="error", value=error_msg)
    raise e