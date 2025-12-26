# Databricks notebook source
# Definir parámetros de entrada del notebook
dbutils.widgets.text("send_to", "cesarg29j@gmail.com", "Enviar al Correo")
dbutils.widgets.text("lista_detalle", "", "Lista de Calidad")

# COMMAND ----------

# Obtener parámetros
send_to = dbutils.widgets.get("send_to")
lista = dbutils.widgets.get("lista_detalle")

# COMMAND ----------

import smtplib
from email.message import EmailMessage
import pandas as pd
from io import StringIO
import json

# COMMAND ----------

def mensaje_calidad_datos(lista_calidad: str):
    mensaje = ""
    lista = json.loads(lista_calidad)
    for i, error in enumerate(lista, start=1):
        mensaje += f"📌Problema de Calidad N° {i}:\n"
        for clave, valor in error.items():
            mensaje += f"  - {clave}: {valor}\n"
        mensaje += "\n"
    return mensaje

# COMMAND ----------

mensaje = mensaje_calidad_datos(lista)

# COMMAND ----------

msg = EmailMessage()
correo_origen = 'edgar.quispeq10@gmail.com'

msg['Subject'] = '❌ Error en la Ejecución del Pipeline ETL'
msg['From'] = correo_origen
msg['To'] = send_to

msg.set_content(mensaje)

# Datos del servidor SMTP de Gmail
smtp_server = 'smtp.gmail.com'
smtp_port = 587
clave_app = 'fbcc mxav rrux otpg'  # Generada en tu cuenta de Google

# Enviar el correo
try:
    with smtplib.SMTP(smtp_server, smtp_port) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(correo_origen, clave_app)
        smtp.send_message(msg)
        smtp.quit()
        print('✅ Correo enviado con éxito.')
except Exception as e:
        print(f'❌ Error al enviar el correo: {e}')