# Databricks notebook source
# Definir parámetros de entrada del notebook
dbutils.widgets.text("send_to", "cursos@addc-peru.com", "Enviar al Correo")
dbutils.widgets.text("mensaje", "", "Mensaje del Correo")

# COMMAND ----------

# Obtener parámetros
send_to = dbutils.widgets.get("send_to")
mensaje = dbutils.widgets.get("mensaje")

# COMMAND ----------

import smtplib
from email.message import EmailMessage
import pandas as pd
from io import StringIO
import json

# COMMAND ----------

msg = EmailMessage()
correo_origen = 'gutierrezjulio710@gmail.com'

msg['Subject'] = '✅ Pipeline ELT Ejecutado Exitosamente'
msg['From'] = correo_origen
msg['To'] = send_to

msg.set_content(mensaje)

# Datos del servidor SMTP de Gmail
smtp_server = 'smtp.gmail.com'
smtp_port = 587
clave_app = 'gisn waxv xybl gvrz'  # Generada en tu cuenta de Google

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