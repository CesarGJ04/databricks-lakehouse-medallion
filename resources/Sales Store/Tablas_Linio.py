# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.linio;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE workspace.linio.bronze_compras (
# MAGIC   venta_id STRING,
# MAGIC   factura STRING,
# MAGIC   fecha_orden STRING,
# MAGIC   fecha_entrega STRING,
# MAGIC   fecha_envio STRING,
# MAGIC   estado STRING,
# MAGIC   cliente_code STRING,
# MAGIC   tipo_cliente STRING,
# MAGIC   nombres STRING,
# MAGIC   apellidos STRING,
# MAGIC   vendedor STRING,
# MAGIC   departamento STRING,
# MAGIC   metodo_pago STRING,
# MAGIC   subtotal STRING,
# MAGIC   tipo_compra STRING,
# MAGIC   fecha_carga TIMESTAMP)
# MAGIC USING delta
# MAGIC LOCATION 'abfss://salesxstore@adlssalesstore.dfs.core.windows.net/bronze/compras'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE workspace.linio.bronze_detalles (
# MAGIC   detalle_id STRING,
# MAGIC   factura STRING,
# MAGIC   num_tracking STRING,
# MAGIC   categoria STRING,
# MAGIC   subcategoria STRING,
# MAGIC   producto STRING,
# MAGIC   unidades STRING,
# MAGIC   precio_unitario STRING,
# MAGIC   oferta_id STRING,
# MAGIC   nombre_archivo STRING,
# MAGIC   fecha_carga TIMESTAMP)
# MAGIC USING delta
# MAGIC LOCATION 'abfss://salesxstore@adlssalesstore.dfs.core.windows.net/bronze/detalles'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE workspace.linio.silver_compras (
# MAGIC   periodo STRING,
# MAGIC   venta_id INT,
# MAGIC   factura STRING,
# MAGIC   tipo_compra STRING,
# MAGIC   fecha_orden DATE,
# MAGIC   fecha_entrega DATE,
# MAGIC   fecha_envio DATE,
# MAGIC   estado STRING,
# MAGIC   cliente_id INT,
# MAGIC   tipo_documento STRING,
# MAGIC   num_documento STRING,
# MAGIC   nombre_cliente STRING,
# MAGIC   tipo_cliente STRING,
# MAGIC   vendedor STRING,
# MAGIC   departamento STRING,
# MAGIC   metodo_pago STRING,
# MAGIC   dias_abierto INT,
# MAGIC   grupo_dias_abierto STRING,
# MAGIC   fecha_carga TIMESTAMP)
# MAGIC USING delta
# MAGIC LOCATION 'abfss://salesxstore@adlssalesstore.dfs.core.windows.net/silver/compras'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE workspace.linio.silver_detalles (
# MAGIC   detalle_id INT,
# MAGIC   factura STRING,
# MAGIC   tienda STRING,
# MAGIC   oferta_id INT,
# MAGIC   categoria STRING,
# MAGIC   subcategoria STRING,
# MAGIC   producto STRING,
# MAGIC   unidades INT,
# MAGIC   subtotal DOUBLE,
# MAGIC   fecha_carga TIMESTAMP)
# MAGIC USING delta
# MAGIC LOCATION 'abfss://salesxstore@adlssalesstore.dfs.core.windows.net/silver/detalles'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE workspace.linio.gold_fact_compras (
# MAGIC   periodo STRING,
# MAGIC   factura STRING,
# MAGIC   venta_id INT,
# MAGIC   fecha_orden DATE,
# MAGIC   fecha_envio DATE,
# MAGIC   estado STRING,
# MAGIC   metodo_pago STRING,
# MAGIC   grupo_dias_abierto STRING,
# MAGIC   cliente_id INT,
# MAGIC   tipo_documento STRING,
# MAGIC   num_documento STRING,
# MAGIC   nombre_cliente STRING,
# MAGIC   tipo_cliente STRING,
# MAGIC   vendedor STRING,
# MAGIC   departamento STRING,
# MAGIC   detalle_id INT,
# MAGIC   tienda STRING,
# MAGIC   categoria STRING,
# MAGIC   subcategoria STRING,
# MAGIC   producto STRING,
# MAGIC   unidades INT,
# MAGIC   subtotal DOUBLE,
# MAGIC   fecha_carga TIMESTAMP)
# MAGIC USING delta
# MAGIC LOCATION 'abfss://salesxstore@adlssalesstore.dfs.core.windows.net/gold/fact-compras'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE workspace.linio.log_calidad_datos (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   tabla STRING,
# MAGIC   columna STRING,
# MAGIC   regla STRING,
# MAGIC   cumple BOOLEAN,
# MAGIC   detalle STRING,
# MAGIC   fecha_validacion TIMESTAMP)
# MAGIC USING delta
# MAGIC LOCATION 'abfss://salesxstore@adlssalesstore.dfs.core.windows.net/logging/calidad-datos'