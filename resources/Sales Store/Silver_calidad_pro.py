# Databricks notebook source
import json
from pyspark.sql.functions import col, length, current_timestamp

# COMMAND ----------

# Importados datos de las tablas delta en la capa Bronze
df_compras = spark.table("workspace.linio.silver_compras")
df_detalles = spark.table("workspace.linio.silver_detalles")

# COMMAND ----------

reglas = []

# COMMAND ----------

df_compras.limit(10).display()

# COMMAND ----------

# 1. No nulos en fecha_envio
n_null_entregado = df_compras.filter(
    (col("estado")=="Entregado") & col("fecha_envio").isNull()).count()
reglas.append({
    "tabla": "silver_compras",
    "columna": "estado",
    "regla": "no_null_si_entregado",
    "cumple": n_null_entregado == 0,
    "detalle": f"Registros Nulos: {n_null_entregado}"
})

# 2. No mayor que fechas reciente a la anterior
difer_entregado = df_compras.filter(
    (col("estado") == "Entregado") & (col("fecha_envio") < col("fecha_orden"))).count()
reglas.append({
    "tabla": "silver_compras",
    "columna": "fecha_envio",
    "regla": "fecha_envio>=fecha_orden",
    "cumple": difer_entregado == 0,
    "detalle":  f"Registros con fecha_envio < fecha_orden (estado='Entregado'): {difer_entregado}"
})

# COMMAND ----------

# 1 Mayor que cero
m_cero_subtotal = df_detalles.filter(col("subtotal") <= 0).count()
reglas.append({
    "tabla": "silver_detalles",
    "columna":"subtotal",
    "regla": "subtotal>_0",
    "cumple": m_cero_subtotal == 0,
    "detalle": f"Registros Nulos <= 0: {m_cero_subtotal}"
})

# COMMAND ----------

df_reglas = spark.createDataFrame(reglas)
df_reglas = df_reglas.select("tabla", "columna", "regla", "cumple", "detalle").withColumn("fecha_validacion", current_timestamp())

# COMMAND ----------

#%sql
#CREATE TABLE IF NOT EXISTS linio.log_calidad_datosSilver
#(
#    id BIGINT GENERATED ALWAYS AS IDENTITY,
#    tabla STRING,
#    columna STRING,
#    regla STRING,
#    cumple BOOLEAN,
#    detalle STRING,
#    fecha_validacion TIMESTAMP
#)

# COMMAND ----------

(
    df_reglas.write.format("delta")
                   .mode("append")
                   .saveAsTable("workspace.linio.log_calidad_datos")
)

# COMMAND ----------

fallas_criticas = [regla for regla in reglas if not regla["cumple"]]

if fallas_criticas:
    dbutils.jobs.taskValues.set(key="estado", value="Falla Critica")
    dbutils.jobs.taskValues.set(key="detalle", value=json.dumps(fallas_criticas))
else:
    dbutils.jobs.taskValues.set("estado", "Validacion Exitosa")