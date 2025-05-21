from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pprint

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]

print("📂 Documento de ejemplo desde 'docs':")
doc = db["docs"].find_one()
pprint.pprint(doc)

print("\n📂 Documento de ejemplo desde 'pagos':")
pag = db["pagos"].find_one()
pprint.pprint(pag)
