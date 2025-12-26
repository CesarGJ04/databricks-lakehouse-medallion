# Databricks notebook source
import json
from pyspark.sql.functions import col, length, current_timestamp

# COMMAND ----------

# Importados datos de las tablas delta en la capa Bronze
df_compras = spark.table("workspace.linio.bronze_compras")
df_detalles = spark.table("workspace.linio.bronze_detalles")

# COMMAND ----------

reglas = []

# COMMAND ----------

# Validacion en la base Compras

# 1. No null en numero_factura
n_null_FacturasCompras = df_compras.filter(col("factura").isNull()).count()
reglas.append({
    "tabla": "bronze_compras",
    "columna": "factura",
    "regla": "no_null",
    "cumple": n_null_FacturasCompras == 0,
    "detalle": f"Registros Nulos: {n_null_FacturasCompras}"
})

#2. Longitud minima en factura
short_ids = df_compras.filter(length(col("factura")) < 7).count()
reglas.append({
    "tabla":"bronze_compras",
    "columna":"factura",
    "regla":"longitud_minima",
    "cumple": short_ids == 0,
    "detalle": f"IDs_cortos: {short_ids}"
})

#3. Datos duplicados
duplicados = df_compras.groupBy("factura").count().filter(col("count") > 1).count()
reglas.append({
    "tabla": "bronze_compras",
    "columna": "factura",
    "regla": "duplicados",
    "cumple": duplicados == 0,
    "detalle": f"Registros Duplicados: {duplicados}"
})
#4. No null en fecha_orden
n_null_fecha = df_compras.filter(col("fecha_orden").isNull()).count()
reglas.append({
    "tabla": "bronze_compras",
    "columna": "fecha_creacion",
    "regla": "no_null",
    "cumple": n_null_fecha == 0,
    "detalle": f"fechas_Nulos: {n_null_fecha}"
})

# COMMAND ----------

reglas

# COMMAND ----------

df_detalles.columns

# COMMAND ----------

# Validacion en la base Detalles

# 1. No null en factura_detalles
n_null_Facturasdetalles = df_detalles.filter(col("factura").isNull()).count()
reglas.append({
    "tabla":"bronze_detalles",
    "columna":"factura",
    "regla":"no_null",
    "cumple": n_null_Facturasdetalles == 0,
    "detalle": f"Facturas nulos: {n_null_Facturasdetalles}"
})
# 2. No null en factura_detalles
n_null_Productodetalles = df_detalles.filter(col("producto").isNull()).count()
reglas.append({
    "tabla":"bronze_detalles",
    "columna":"producto",
    "regla":"no_null",
    "cumple": n_null_Productodetalles == 0,
    "detalle": f"Productos nulos: {n_null_Productodetalles}"
})

# COMMAND ----------

reglas

# COMMAND ----------

df_reglas = spark.createDataFrame(reglas)
df_reglas.display()

# COMMAND ----------

df_reglas = df_reglas.select("tabla", "columna", "regla", "cumple", "detalle").withColumn("fecha_validacion", current_timestamp())

# COMMAND ----------

#%sql
#CREATE TABLE IF NOT EXISTS linio.log_calidad_datos
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

fallas_criticas = reglas

# COMMAND ----------

fallas_criticas = [regla for regla in reglas if not regla["cumple"]]

if fallas_criticas:
    dbutils.jobs.taskValues.set(key="estado", value="Falla Critica")
    dbutils.jobs.taskValues.set(key="detalle", value=json.dumps(fallas_criticas))
else:
    dbutils.jobs.taskValues.set("estado", "Validacion Exitosa")