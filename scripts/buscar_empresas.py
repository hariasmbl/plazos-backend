from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Conectar a MongoDB
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]
empresa = db["empresa"]

# Buscar RUT exacto
rut_a_buscar = "82225800-k"
resultado = empresa.find_one({"rut": rut_a_buscar})

# Mostrar resultado
if resultado:
    print("✅ RUT encontrado en la colección 'empresa':")
    print(resultado)
else:
    print("❌ RUT no encontrado en la colección 'empresa'.")
