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

# Calcular hash por campos clave únicamente
def calcular_hash(fila):
    claves_relevantes = ["RUT DEUDOR", "Nº DCTO", "Nº OPE"]
    datos = tuple((clave, fila.get(clave)) for clave in claves_relevantes)
    return hashlib.sha256(str(sorted(datos)).encode("utf-8")).hexdigest()

# Leer archivo Excel
def cargar_excel(path):
    try:
        df = pd.read_excel(path, engine="openpyxl")
        print(f"\n📄 {path} cargado con {len(df)} filas")
        return df
    except Exception as e1:
        try:
            df = pd.read_excel(path, engine="xlrd")
            print(f"\n📄 {path} cargado con {len(df)} filas (xlrd)")
            return df
        except Exception as e2:
            print(f"❌ Error leyendo {path}: {e2}")
            return pd.DataFrame()

# Insertar datos en MongoDB con control de duplicados y actualización si cambia estado
def insertar_documentos(df, nombre_archivo):
    total, nuevos, duplicados, actualizados = len(df), 0, 0, 0
    coleccion.create_index("_hash", unique=True)

    for _, fila in df.iterrows():
        doc = fila.to_dict()
        doc["_hash"] = calcular_hash(doc)
        doc["origen_archivo"] = nombre_archivo
        doc["origen_tipo"] = "docs"

        criterio = {
            "RUT DEUDOR": doc.get("RUT DEUDOR"),
            "Nº DCTO": doc.get("Nº DCTO"),
            "Nº OPE": doc.get("Nº OPE")
        }

        existente = coleccion.find_one(criterio)

        if existente:
            if existente.get("ESTADO") != doc.get("ESTADO"):
                coleccion.update_one({"_id": existente["_id"]}, {"$set": doc})
                actualizados += 1
            else:
                duplicados += 1
        else:
            try:
                coleccion.insert_one(doc)
                nuevos += 1
            except:
                duplicados += 1

    print(f"✅ Insertados: {nuevos} | 🔁 Duplicados: {duplicados} | 🔄 Actualizados: {actualizados}")

# --------------------------
# Procesar archivos tipo "list docs"
# --------------------------

if __name__ == "__main__":
    carpeta_data = "data"
    archivos = [
        os.path.join(carpeta_data, f)
        for f in os.listdir(carpeta_data)
        if f.lower().endswith((".xls", ".xlsx")) and "list docs" in f.lower() and "procesados" not in f.lower()
    ]

    print(f"\n🧾 Archivos encontrados: {len(archivos)}")
    if not archivos:
        print("⚠️ No se encontraron archivos nuevos para procesar.")
    
    for archivo in archivos:
        df = cargar_excel(archivo)
        if not df.empty:
            insertar_documentos(df, os.path.basename(archivo))
            # Mover a carpeta de procesados
            destino = os.path.join(carpeta_data, "procesados")
            os.makedirs(destino, exist_ok=True)
            nuevo_path = os.path.join(destino, os.path.basename(archivo))
            shutil.move(archivo, nuevo_path)
            print(f"📦 Archivo movido a: {nuevo_path}")
