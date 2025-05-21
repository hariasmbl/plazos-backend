import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import hashlib
import os
import shutil

# Configuración Mongo
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]
coleccion = db["docs"]

# Calcular hash por fila
def calcular_hash(fila):
    fila_str = str(sorted(fila.items()))
    return hashlib.sha256(fila_str.encode("utf-8")).hexdigest()

# Leer archivo Excel
def cargar_excel(path):
    try:
        df = pd.read_excel(path, engine='openpyxl' if path.endswith('xlsx') else 'xlrd')
        print(f"\n📄 {path} cargado con {len(df)} filas")
        return df
    except Exception as e:
        print(f"❌ Error leyendo {path}: {e}")
        return pd.DataFrame()

# Insertar datos en MongoDB con control de duplicados
def insertar_documentos(df, nombre_archivo):
    total, nuevos, duplicados = len(df), 0, 0
    coleccion.create_index("_hash", unique=True)

    for _, fila in df.iterrows():
        doc = fila.to_dict()
        doc["_hash"] = calcular_hash(doc)
        doc["origen_archivo"] = nombre_archivo
        doc["origen_tipo"] = "docs"

        try:
            coleccion.insert_one(doc)
            nuevos += 1
        except:
            duplicados += 1

    print(f"✅ Insertados: {nuevos} | 🔁 Duplicados: {duplicados}")

# Mover archivo a /procesados
def mover_a_procesados(path):
    destino = os.path.join("data", "procesados")
    os.makedirs(destino, exist_ok=True)
    nuevo_path = os.path.join(destino, os.path.basename(path))
    shutil.move(path, nuevo_path)
    print(f"📦 Archivo movido a: {nuevo_path}")

# Detectar archivos tipo "list docs"
archivos = [
    os.path.join("data", f)
    for f in os.listdir("data")
    if f.lower().endswith((".xls", ".xlsx")) and "list docs" in f.lower() and "procesados" not in f.lower()
]

# Procesar archivos uno a uno
for archivo in archivos:
    if os.path.exists(archivo):
        df = cargar_excel(archivo)
        if not df.empty:
            insertar_documentos(df, os.path.basename(archivo))
            mover_a_procesados(archivo)
    else:
        print(f"⚠️ Archivo no encontrado: {archivo}")
