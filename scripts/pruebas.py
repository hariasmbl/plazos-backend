from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"))
db = client["mi_base_datos"]

empresas = db["empresas"]
docs = db["docs"]
pagos = db["pagos"]

# Buscar la empresa
rut_consultado = "76107905-0"
empresa = empresas.find_one({"rut": rut_consultado})
if not empresa:
    print("Empresa no encontrada.")
    exit()

rubro = empresa["rubro"]
tramo = empresa["tramo_ventas"]

# Buscar RUTs similares
similares = list(empresas.find({"rubro": rubro, "tramo_ventas": tramo}))
ruts_similares = [e["rut"] for e in similares]

# Buscar facturas de empresas similares
facturas_similares = list(docs.find({"RUT DEUDOR": {"$in": ruts_similares}}))
pagos_similares = list(pagos.find({"Rut Deudor": {"$in": ruts_similares}, "Estado": "PAGADO"}))

print(f"Empresas similares encontradas: {len(similares)}")
print(f"Facturas similares encontradas: {len(facturas_similares)}")
print(f"Pagos similares encontrados: {len(pagos_similares)}")
