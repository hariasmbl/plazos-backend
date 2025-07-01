import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Cargar configuración
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]
empresas = db["empresas"]

# Ruta al archivo (usa r'' para evitar errores por espacio)
ruta = r'C:\Users\Damsoft\Desktop\Plazos\Otros_docs\PUB_EMPRESAS.txt'

# Leer solo columnas necesarias
columnas = [
    "Año comercial", "RUT", "DV", "Razón social",
    "Tramo según ventas", "Rubro económico"
]

df = pd.read_csv(ruta, sep="\t", encoding="utf-8", usecols=columnas, low_memory=False)
df = df[df["Año comercial"] == 2023]

# Eliminar duplicados por RUT
df["rut"] = df["RUT"].astype(str) + "-" + df["DV"].astype(str)
df = df.drop_duplicates(subset="rut")

registros = []
for _, row in df.iterrows():
    registros.append({
        "rut": row["rut"],
        "nombre": str(row["Razón social"]).strip(),
        "tramo_ventas": str(row["Tramo según ventas"]).strip(),
        "rubro": str(row["Rubro económico"]).strip()
    })

# Insertar en MongoDB
empresas.drop()  # opcional: limpiar antes de insertar
empresas.insert_many(registros)
print(f"{len(registros)} empresas cargadas.")

def procesar_txt(ruta):
    columnas = [
        "Año comercial", "RUT", "DV", "Razón social",
        "Tramo según ventas", "Rubro económico"
    ]
    df = pd.read_csv(ruta, sep="\t", encoding="utf-8", usecols=columnas, low_memory=False)
    df = df[df["Año comercial"] == 2023]
    df["rut"] = df["RUT"].astype(str) + "-" + df["DV"].astype(str)
    df = df.drop_duplicates(subset="rut")
    registros = []
    for _, row in df.iterrows():
        registros.append({
            "rut": row["rut"],
            "nombre": str(row["Razón social"]).strip(),
            "tramo_ventas": str(row["Tramo según ventas"]).strip(),
            "rubro": str(row["Rubro económico"]).strip()
        })
    empresas = MongoClient(MONGO_URI)["mi_base_datos"]["empresas"]
    empresas.drop()
    empresas.insert_many(registros)
    print(f"{len(registros)} empresas insertadas.")

