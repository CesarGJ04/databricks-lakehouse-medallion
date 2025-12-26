CREATE OR REPLACE TABLE workspace.linio.bronze_compras (
  venta_id STRING,
  factura STRING,
  fecha_orden STRING,
  fecha_entrega STRING,
  fecha_envio STRING,
  estado STRING,
  cliente_code STRING,
  tipo_cliente STRING,
  nombres STRING,
  apellidos STRING,
  vendedor STRING,
  departamento STRING,
  metodo_pago STRING,
  subtotal STRING,
  tipo_compra STRING,
  fecha_carga TIMESTAMP)
USING delta
LOCATION '';

CREATE OR REPLACE TABLE workspace.linio.bronze_details (
  detalle_id STRING,
  factura STRING,
  num_tracking STRING,
  categoria STRING,
  subcategoria STRING,
  producto_id STRING,
  producto STRING,
  unidades STRING,
  precio_unitario STRING,
  oferta_id STRING,
  nombre_archivo STRING,
  fecha_carga TIMESTAMP)
USING delta
LOCATION '';

CREATE OR REPLACE TABLE workspace.linio.silver_compras (
  venta_id INT,
  factura STRING,
  fecha_orden DATE,
  fecha_entrega DATE,
  fecha_envio DATE,
  estado STRING,
  cliente_code STRING,
  tipo_cliente STRING,
  nombres STRING,
  apellidos STRING,
  vendedor STRING,
  departamento STRING,
  metodo_pago STRING,
  tipo_compra STRING,
  cliente_id INT,
  num_documento STRING,
  tipo_documento STRING,
  nombre_cliente STRING,
  dias_abierto INT,
  grupo_dias_abierto STRING)
USING delta
LOCATION '';


CREATE OR REPLACE TABLE workspace.linio.silver_details (
  detalle_id INT,
  factura STRING,
  oferta_id INT,
  categoria STRING,
  subcategoria STRING,
  producto STRING,
  unidades INT,
  subtotal DOUBLE,
  tienda STRING,
  fecha_carga TIMESTAMP)
USING delta
LOCATION '';


CREATE OR REPLACE TABLE workspace.linio.log_calidad_datos (
  id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  tabla STRING,
  columna STRING,
  regla STRING,
  cumple BOOLEAN,
  detalle STRING,
  fecha_validacion TIMESTAMP)
USING delta
LOCATION '';


CREATE OR REPLACE TABLE workspace.linio.gold_compras (
  venta_id INT,
  factura STRING,
  fecha_orden DATE,
  fecha_envio DATE,
  estado STRING,
  metodo_pago STRING,
  grupo_dias_abierto STRING,
  cliente_id INT,
  tipo_documento STRING,
  num_documento STRING,
  nombre_cliente STRING,
  tipo_cliente STRING,
  vendedor STRING,
  departamento STRING,
  detalle_id INT,
  tienda STRING,
  categoria STRING,
  subcategoria STRING,
  producto STRING,
  unidades INT,
  subtotal DOUBLE,
  fecha_carga TIMESTAMP)
USING delta
LOCATION ''