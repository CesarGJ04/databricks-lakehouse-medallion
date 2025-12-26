# Databricks notebook source
# Definir parámetros de entrada del notebook
dbutils.widgets.text("send_to", "cursos@addc-peru.com", "Enviar al Correo")

# COMMAND ----------

# Obtener parámetros
send_to = dbutils.widgets.get("send_to")

# COMMAND ----------

import smtplib
from email.message import EmailMessage
import pandas as pd
from io import StringIO
import json

# COMMAND ----------

lista_tasks = ["Bronze_Layer", "Silver_Layer", "Gold_Layer"]

# COMMAND ----------

mensaje = ""
errores = []
lista = json.loads(lista_tasks)
for task in lista:
    try:
        err = dbutils.jobs.taskValues.get(taskKey=task, key="error", debugValue=None)
        if err:
            errores.append(f"❌ {task}: {err}")
    except:
        pass  # La tarea no falló o no registró mensaje

if errores:
    mensaje = "🚨 Se detectaron errores en las siguientes tareas:\n\n" + "\n".join(errores)
else:
    mensaje = "⚠️ Se detectó un error en el pipeline, pero no se encontró detalle del error."

# COMMAND ----------

msg = EmailMessage()
correo_origen = 'gutierrezjulio710@gmail.com'

msg['Subject'] = '❌ Error en la Ejecución del Pipeline ETL'
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