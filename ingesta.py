import boto3
import mysql.connector
import csv

# ==== CONFIGURACIÓN MYSQL ====
db_config = {
    "host": "TU_HOST",        # Ejemplo: "localhost" o endpoint RDS
    "user": "TU_USUARIO",
    "password": "TU_PASSWORD",
    "database": "TU_BASE_DATOS"
}

# ==== CONFIGURACIÓN S3 ====
ficheroUpload = "data.csv"
nombreBucket = "gcr-output-01"

# ==== CONECTAR A MYSQL ====
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Cambia 'tu_tabla' por el nombre de tu tabla
    cursor.execute("SELECT * FROM tu_tabla")
    rows = cursor.fetchall()

    # Obtener nombres de columnas
    columnas = [i[0] for i in cursor.description]

    # ==== GUARDAR EN CSV ====
    with open(ficheroUpload, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columnas)  # cabeceras
        writer.writerows(rows)

    print(f"Archivo {ficheroUpload} creado con {len(rows)} registros.")

except Exception as e:
    print("Error al conectar/leer la base de datos:", e)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals() and conn.is_connected():
        conn.close()

# ==== SUBIR A S3 ====
try:
    s3 = boto3.client('s3')
    s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)
    print("Ingesta completada y archivo subido a S3.")

except Exception as e:
    print("Error al subir a S3:", e)
