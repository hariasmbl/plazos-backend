import os
import shutil
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import hashlib

# Cargar variables de entorno
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]


def calcular_hash_doc(fila):
    claves_relevantes = ["RUT DEUDOR", "Nº DCTO", "Nº OPE"]
    datos = tuple((clave, fila.get(clave)) for clave in claves_relevantes)
    return hashlib.sha256(str(sorted(datos)).encode("utf-8")).hexdigest()


def procesar_list_docs(path):
    coleccion = db["docs"]
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except:
        df = pd.read_excel(path, engine="xlrd")

    total, nuevos, duplicados, actualizados = len(df), 0, 0, 0
    coleccion.create_index("_hash", unique=True)

    for _, fila in df.iterrows():
        doc = fila.to_dict()
        doc["_hash"] = calcular_hash_doc(doc)
        doc["origen_archivo"] = os.path.basename(path)
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

    return f"List docs: Insertados {nuevos}, Duplicados {duplicados}, Actualizados {actualizados}"


def calcular_hash_pago(fila):
    fila_str = str(sorted(fila.items()))
    return hashlib.sha256(fila_str.encode("utf-8")).hexdigest()


def procesar_cartola(path):
    coleccion = db["pagos"]
    try:
        xls = pd.ExcelFile(path)
        hoja_objetivo = next((h for h in xls.sheet_names if "cartola" in h.lower()), xls.sheet_names[0])
        df = pd.read_excel(xls, sheet_name=hoja_objetivo, header=4)
        df.columns = df.columns.str.strip().str.replace(r'\s+', ' ', regex=True)
    except Exception as e:
        return f"Error leyendo cartola: {e}"

    columnas = ["Tipo Pago", "Det. Pago", "Tipo Prod.", "Rut Cliente", "Rut Deudor", "Fecha Pago", "Mto.Pagado"]
    if not all(col in df.columns for col in columnas):
        return "Cartola inválida: faltan columnas requeridas."

    df = df[df["Tipo Prod."].astype(str).str.upper() != "TOTAL CLIENTE"]
    df = df[df["Tipo Pago"].astype(str).str.upper() == "RECAUDACION"]
    df = df[df["Det. Pago"].astype(str).str.upper() == "DEUDOR"]
    df = df.dropna(subset=["Rut Cliente", "Rut Deudor", "Fecha Pago", "Mto.Pagado"])

    coleccion.create_index("_hash", unique=True)
    nuevos, duplicados = 0, 0

    for _, fila in df.iterrows():
        doc = fila.to_dict()
        doc["_hash"] = calcular_hash_pago(doc)
        doc["origen_archivo"] = os.path.basename(path)
        doc["origen_tipo"] = "pagos"
        try:
            coleccion.insert_one(doc)
            nuevos += 1
        except:
            duplicados += 1

    return f"Cartola: Insertados {nuevos}, Duplicados {duplicados}"


def procesar_txt_empresas(path):
    empresas = db["empresas"]
    columnas = ["Año comercial", "RUT", "DV", "Razón social", "Tramo según ventas", "Rubro económico"]
    df = pd.read_csv(path, sep="\t", encoding="utf-8", usecols=columnas, low_memory=False)
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

    empresas.drop()
    empresas.insert_many(registros)
    return f"Empresas: {len(registros)} registros cargados."
