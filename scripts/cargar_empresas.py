import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

COLUMNAS = [
    "Año comercial", "RUT", "DV", "Razón social",
    "Tramo según ventas", "Rubro económico"
]


def procesar_txt(ruta):
    client = MongoClient(MONGO_URI)
    empresas = client["mi_base_datos"]["empresas"]

    df = pd.read_csv(ruta, sep="\t", encoding="utf-8", usecols=COLUMNAS, low_memory=False)
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
    print(f"{len(registros)} empresas insertadas desde archivo: {ruta}")


if __name__ == "__main__":
    ruta = r'C:\Users\Damsoft\Desktop\Plazos\Otros_docs\PUB_EMPRESAS.txt'
    procesar_txt(ruta)
