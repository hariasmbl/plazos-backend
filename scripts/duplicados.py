from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]
coleccion = db["docs"]

# Agrupamos por RUT DEUDOR, número de documento y número de operación
duplicados = coleccion.aggregate([
    {
        "$group": {
            "_id": {
                "RUT DEUDOR": "$RUT DEUDOR",
                "numero": "$Nº DCTO",
                "ope": "$Nº OPE"
            },
            "conteo": {"$sum": 1}
        }
    },
    {
        "$match": {"conteo": {"$gt": 1}}
    }
])

print("🔎 Encontrados los siguientes casos de facturas duplicadas:")
for d in duplicados:
    print(f"- RUT: {d['_id']['RUT DEUDOR']}, Nº: {d['_id']['numero']}, Nº OPE: {d['_id']['ope']}, Repeticiones: {d['conteo']}")
