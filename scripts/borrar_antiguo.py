from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Cargar config desde .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Conexión
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]

# Borrar colección anterior desordenada
if "mis_datos" in db.list_collection_names():
    db["mis_datos"].drop()
    print("🗑️ Colección 'mis_datos' eliminada correctamente.")
else:
    print("ℹ️ No existe la colección 'mis_datos'. Nada que eliminar.")
