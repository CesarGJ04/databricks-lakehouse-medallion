# Databricks notebook source
from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

#tables = spark.catalog.listTables("workspace.linio")
#for table in tables:
#    spark.sql(
#        f"DROP TABLE IF EXISTS workspace.linio.{table.name}"
#    )

# COMMAND ----------

# rutas del los archivos a importar
file_path_compras_online     = r"abfss://salesxstore@adlssalesstore.dfs.core.windows.net/landing/compras/Online.json"
file_path_compras_presencial = r"abfss://salesxstore@adlssalesstore.dfs.core.windows.net/landing/compras/Presencial.csv"
file_path_detalles            = r"abfss://salesxstore@adlssalesstore.dfs.core.windows.net/landing/detalles"

# COMMAND ----------

compras_presencial = (
                spark.read.format("csv")
                          .option("header","true")
                          .option("sep",";")
                          .option("inferSchema","false")
                          .load(file_path_compras_presencial)
                          )

# COMMAND ----------

compras_presencial.limit(10).display()

# COMMAND ----------

compras_presencial = (
                spark.read.format("csv")
                          .option("header","true")
                          .option("sep",";")
                          .option("inferSchema","false")
                          .load(file_path_compras_presencial)
                          )
compras_presencial = (compras_presencial
                        .withColumnRenamed('VentaID','venta_id')
                        .withColumnRenamed('Factura','factura')
                        .withColumnRenamed('Fecha_Orden','fecha_orden')
                        .withColumnRenamed('Fecha_Entrega','fecha_entrega')
                        .withColumnRenamed('Fecha_Envio','fecha_envio')
                        .withColumnRenamed('Estado','estado')
                        .withColumnRenamed('Cliente_Code','cliente_code')
                        .withColumnRenamed('Tipo_Cliente','tipo_cliente')
                        .withColumnRenamed('Nombres','nombres')
                        .withColumnRenamed('Apellidos','apellidos')
                        .withColumnRenamed('Vendedor','vendedor')
                        .withColumnRenamed('Departamento','departamento')
                        .withColumnRenamed('Metodo_Pago','metodo_pago')
                        .withColumnRenamed('Subtotal','subtotal')
                        )
compras_presencial = compras_presencial.withColumn('tipo_compra',lit("Presencial"))\
.withColumn('fecha_carga', current_timestamp())                 

# COMMAND ----------

compras_online = (spark.read.format("json")
                .option("multiline","true")
                .load(file_path_compras_online)
                )
compras_online.limit(10).display()

# COMMAND ----------

compras_online = (spark.read.format("json")
                .option("multiline","true")
                .load(file_path_compras_online)
                )
compras_online = (compras_online
                .withColumnRenamed('Apellidos','apellidos')
                .withColumnRenamed('ClienteCode','cliente_code')
                .withColumnRenamed('Departamento','departamento')
                .withColumnRenamed('Estado','estado')
                .withColumnRenamed('Factura','factura')
                .withColumnRenamed('FechaEntrega','fecha_entrega')
                .withColumnRenamed('FechaEnvio','fecha_envio')
                .withColumnRenamed('FechaOrden','fecha_orden')
                .withColumnRenamed('MetodoPago','metodo_pago')
                .withColumnRenamed('Nombres','nombres')
                .withColumnRenamed('TipoCliente','tipo_cliente')
                .withColumnRenamed('Vendedor','vendedor')
                .withColumnRenamed('VentaID', 'venta_id')
                )
compras_online =(compras_online
                .withColumn('tipo_compra', lit("Online"))\
                .withColumn('fecha_carga',current_timestamp())                
                )

# COMMAND ----------

df_compras = compras_presencial.unionByName(compras_online, allowMissingColumns= True)

# COMMAND ----------

df_compras.limit(10).display()

# COMMAND ----------

df_detalles = (
           spark.read.format("CSV")
           .option("header",True)
           .option("inferSchema","false")
           .option("sep","|")
           .load(file_path_detalles)
          )
df_detalles.limit(10).display()          

# COMMAND ----------

df_detalles = (
           spark.read.format("CSV")
           .option("header",True)
           .option("inferSchema","false")
           .option("sep","|")
           .load(file_path_detalles)
          )
df_detalles = (
             df_detalles
            .withColumnRenamed('Detalle_ID','detalle_id')
            .withColumnRenamed('Factura','factura')
            .withColumnRenamed('Num_Tracking','num_tracking')
            .withColumnRenamed('Categoria','categoria')
            .withColumnRenamed('Subcategoria','subcategoria')
            .withColumnRenamed('Producto_ID','producto_id')
            .withColumnRenamed('Producto','producto')
            .withColumnRenamed('Unidades','unidades')
            .withColumnRenamed('Precio_Unitario','precio_unitario')
            .withColumnRenamed('Oferta_ID','oferta_id')
             )
df_detalles = df_detalles.withColumn('nombre_archivo', col("_metadata.file_name"))\
                       .withColumn('fecha_carga', current_timestamp())             

# COMMAND ----------

df_detalles = df_detalles.drop("producto_id")

# COMMAND ----------

df_detalles.limit(10).display()

# COMMAND ----------

df_detalles.columns

# COMMAND ----------

try:
    (
    df_compras.write
              .format("delta")
              .mode("overwrite")
              .saveAsTable("workspace.linio.bronze_compras")
    )
    (
    df_detalles.write
              .format("delta")
              .mode("overwrite")
              .saveAsTable("workspace.linio.bronze_detalles")
    )
    dbutils.jobs.taskValues.set(key="estado" , value= "ok")

except Exception as e:
   import traceback
   # Tipo error:
   error_type = type(e).__name__
   # Error sumare:
   error_summary = str(e)
   # Traza del error:
   error_trace = traceback.format_exc()

   # Error completo:
   error_msg_full = f"{error_type}: {error_summary}\n{error_trace}"

   if len(error_msg_full) > 500:
        error_msg = error_msg_full[:500] + "\n[...] ERROR TRUNCADO [...]"
   else:
        error_msg = error_msg_full

   dbutils.jobs.taskValues.set(key="error" , value= error_msg)
   raise e