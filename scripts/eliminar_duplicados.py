from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]
coleccion = db["docs"]

# Agrupar por claves únicas y contar duplicados
pipeline = [
    {
        "$group": {
            "_id": {
                "RUT DEUDOR": "$RUT DEUDOR",
                "Nº DCTO": "$Nº DCTO",
                "Nº OPE": "$Nº OPE"
            },
            "ids": {"$push": "$_id"},
            "count": {"$sum": 1}
        }
    },
    {"$match": {"count": {"$gt": 1}}}
]

resultados = list(coleccion.aggregate(pipeline))

eliminados = 0

for grupo in resultados:
    ids = grupo["ids"]
    # Mantener solo el primero y eliminar el resto
    for id_duplicado in ids[1:]:
        coleccion.delete_one({"_id": id_duplicado})
        eliminados += 1

print(f"🗑️ Duplicados eliminados: {eliminados}")
