from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"))
db = client["mi_base_datos"]

if "pagos" in db.list_collection_names():
    db["pagos"].drop()
    print("🗑️ Colección 'pagos' eliminada completamente.")
else:
    print("ℹ️ La colección 'pagos' no existe.")
