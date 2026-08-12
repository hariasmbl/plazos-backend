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

CHUNKSIZE = 20000


def procesar_txt(ruta):
    """
    Reemplaza por completo la colección 'empresas' a partir del TXT del SII.

    - Se lee en chunks (el archivo puede pesar >1 GB) para no cargar todo en
      memoria; el dedup por RUT se hace a mano contra un set en vez de con
      drop_duplicates sobre el DataFrame completo.
    - Se carga primero a una colección de staging y solo se reemplaza
      'empresas' si el archivo completo se proceso sin errores (rename
      atomico), para no dejar la coleccion a medias si algo falla a mitad
      de camino.

    Devuelve la cantidad de empresas cargadas. Lanza una excepción si el
    archivo no se pudo procesar o no contiene registros válidos.
    """
    client = MongoClient(MONGO_URI)
    db = client["mi_base_datos"]
    staging = db["empresas_staging"]
    staging.drop()

    ruts_vistos = set()
    total = 0

    try:
        for chunk in pd.read_csv(ruta, sep="\t", encoding="utf-8", usecols=COLUMNAS, chunksize=CHUNKSIZE):
            chunk = chunk[chunk["Año comercial"] == 2023]
            if chunk.empty:
                continue

            chunk["rut"] = chunk["RUT"].astype(str) + "-" + chunk["DV"].astype(str)

            registros = []
            for _, row in chunk.iterrows():
                rut = row["rut"]
                if rut in ruts_vistos:
                    continue
                ruts_vistos.add(rut)
                registros.append({
                    "rut": rut,
                    "nombre": str(row["Razón social"]).strip(),
                    "tramo_ventas": str(row["Tramo según ventas"]).strip(),
                    "rubro": str(row["Rubro económico"]).strip()
                })

            if registros:
                staging.insert_many(registros)
                total += len(registros)
    except Exception:
        staging.drop()
        raise

    if total == 0:
        staging.drop()
        raise ValueError("El archivo no contiene registros válidos para el año comercial 2023.")

    staging.rename("empresas", dropTarget=True)
    print(f"{total} empresas insertadas desde archivo: {ruta}")
    return total


if __name__ == "__main__":
    ruta = r'C:\Users\Damsoft\Desktop\Plazos\Otros_docs\PUB_EMPRESAS.txt'
    procesar_txt(ruta)
