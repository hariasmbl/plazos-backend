import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import hashlib
import os
import shutil

# Conexión MongoDB
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]
coleccion = db["pagos"]

# Crear hash único por fila
def calcular_hash(fila):
    fila_str = str(sorted(fila.items()))
    return hashlib.sha256(fila_str.encode("utf-8")).hexdigest()

# Cargar y limpiar archivo Excel
def cargar_y_limpiar_excel(path):
    try:
        xls = pd.ExcelFile(path)
        hojas = xls.sheet_names
        hoja_objetivo = next((h for h in hojas if "cartola" in h.lower()), hojas[0])
        df = pd.read_excel(xls, sheet_name=hoja_objetivo, header=4)

        # ✅ Limpiar y normalizar nombres de columnas
        df.columns = df.columns.str.strip().str.replace(r'\s+', ' ', regex=True)

    except Exception as e:
        print(f"❌ Error al leer {path}: {e}")
        return pd.DataFrame()

    # Validar columnas requeridas
    columnas_requeridas = ["Tipo Pago", "Det. Pago", "Tipo Prod.", "Rut Cliente", "Rut Deudor", "Fecha Pago", "Mto.Pagado"]
    for col in columnas_requeridas:
        if col not in df.columns:
            print(f"⚠️ Faltan columnas requeridas en {path}. Columna ausente: '{col}'")
            return pd.DataFrame()

    # Filtros
    df = df[df["Tipo Prod."].astype(str).str.upper() != "TOTAL CLIENTE"]
    df = df[df["Tipo Pago"].astype(str).str.upper() == "RECAUDACION"]
    df = df[df["Det. Pago"].astype(str).str.upper() == "DEUDOR"]
    df = df.dropna(subset=["Rut Cliente", "Rut Deudor", "Fecha Pago", "Mto.Pagado"])

    print(f"\n📄 {path} cargado con {len(df)} filas válidas")
    return df

# Insertar documentos en MongoDB
def insertar_documentos(df, nombre_archivo):
    total, nuevos, duplicados = len(df), 0, 0
    coleccion.create_index("_hash", unique=True)

    for _, fila in df.iterrows():
        doc = fila.to_dict()
        doc["_hash"] = calcular_hash(doc)
        doc["origen_archivo"] = nombre_archivo
        doc["origen_tipo"] = "pagos"

        try:
            coleccion.insert_one(doc)
            nuevos += 1
        except:
            duplicados += 1

    print(f"✅ Insertados: {nuevos} | 🔁 Duplicados: {duplicados}")

    return {
        "nuevos": nuevos,
        "duplicados": duplicados,
        "actualizados": 0  # Pagos no se actualizan
    }


# Mover archivo procesado
def mover_a_procesados(path):
    destino = os.path.join("data", "procesados")
    os.makedirs(destino, exist_ok=True)
    nuevo_path = os.path.join(destino, os.path.basename(path))
    shutil.move(path, nuevo_path)
    print(f"📦 Archivo movido a: {nuevo_path}")

# Buscar archivos válidos para carga
archivos = [
    os.path.join("data", f)
    for f in os.listdir("data")
    if f.lower().endswith((".xls", ".xlsx"))
    and "procesados" not in f.lower()
    and "list docs" not in f.lower()
]

# Procesar archivos
for archivo in archivos:
    if os.path.exists(archivo):
        df = cargar_y_limpiar_excel(archivo)
        if not df.empty:
            insertar_documentos(df, os.path.basename(archivo))
            mover_a_procesados(archivo)
    else:
        print(f"⚠️ Archivo no encontrado: {archivo}")
