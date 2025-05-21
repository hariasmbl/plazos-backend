from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Cargar variables desde .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI, tls=True, tlsAllowInvalidCertificates=True)


try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    dbs = client.list_database_names()
    print("✅ Conexión exitosa. Bases de datos disponibles:")
    print(dbs)
except Exception as e:
    print("❌ Error de conexión con MongoDB:")
    print(e)
