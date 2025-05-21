from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import numpy as np
import os
import hashlib

# Cargar configuración desde .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]
docs = db["docs"]
pagos = db["pagos"]

# -----------------------------
# Utilidades
# -----------------------------

def parse_fecha(fecha):
    if isinstance(fecha, str):
        for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(fecha.strip(), fmt)
            except:
                continue
    elif isinstance(fecha, datetime):
        return fecha
    return None

def es_outlier(valor, promedio, desviacion):
    if desviacion == 0:
        return False
    z = (valor - promedio) / desviacion
    return abs(z) > 2.0

# -----------------------------
# Función principal
# -----------------------------

def consultar_por_rut(rut_deudor):
    print(f"\n📋 Consultando información para RUT DEUDOR: {rut_deudor}")

    # Obtener documentos
    facturas = list(docs.find({"RUT DEUDOR": rut_deudor}))
    if not facturas:
        print("❌ No se encontraron documentos para este RUT.")
        return

    # Obtener pagos
    pagos_deudor = list(pagos.find({"Rut Deudor": rut_deudor}))
    pagos_dict = {
        (p.get("Nª Doc."), p.get("Nº Ope.")): p for p in pagos_deudor
    }

    registros_validos = []
    for f in facturas:
        clave = (f.get("Nº DCTO"), f.get("Nº OPE"))
        pago = pagos_dict.get(clave)
        if pago:
            fec_emision = parse_fecha(f.get("FEC EMISION DIG"))
            fec_pago = parse_fecha(pago.get("Fecha Pago"))
            fecha_ces = parse_fecha(f.get("FECHA CES"))
            monto = f.get("MONTO DOC")
            if fec_emision and fec_pago:
                plazo = (fec_pago - fec_emision).days
                registros_validos.append({
                    "fecha_ces": fecha_ces,
                    "fecha_emision": fec_emision,
                    "fecha_pago": fec_pago,
                    "plazo": plazo,
                    "monto": monto
                })

    if not registros_validos:
        print("⚠️ No se encontraron coincidencias entre documentos y pagos.")
        return

    # Cálculo de estadísticas
    plazos = [r["plazo"] for r in registros_validos]
    promedio_total = np.mean(plazos)
    desviacion_total = np.std(plazos)

    # Excluir outliers
    registros_limpios = [
        r for r in registros_validos if not es_outlier(r["plazo"], promedio_total, desviacion_total)
    ]

    if not registros_limpios:
        print("⚠️ Todos los registros fueron considerados outliers.")
        return

    # Últimos 5 pagos
    registros_limpios.sort(key=lambda x: x["fecha_pago"], reverse=True)
    ultimos_5 = registros_limpios[:5]
    promedio_ultimos = np.mean([r["plazo"] for r in ultimos_5])

    # Factura con mayor demora
    factura_lenta = max(registros_limpios, key=lambda x: x["plazo"])

    # -----------------------------
    # Resultados
    # -----------------------------
    print(f"\n🧾 Últimos {len(ultimos_5)} pagos:")
    for r in ultimos_5:
        print(f"- Monto: {r['monto']:,.0f} | Compra: {r['fecha_ces'].date() if r['fecha_ces'] else 'N/A'} | "
              f"Emisión: {r['fecha_emision'].date()} | Pago: {r['fecha_pago'].date()} | Plazo: {r['plazo']} días")

    print(f"\n📈 Promedio de plazo (últimos {len(ultimos_5)}): {promedio_ultimos:.2f} días")

    print(f"\n📊 Promedio histórico (sin outliers):")
    print(f"- Pagos considerados: {len(registros_limpios)}")
    print(f"- Promedio de plazo: {np.mean([r['plazo'] for r in registros_limpios]):.2f} días")
    print(f"- Desviación estándar: {desviacion_total:.2f}")

    print(f"\n⏱️ Factura con mayor plazo de pago:")
    print(f"- Monto: {factura_lenta['monto']:,.0f}")
    print(f"- Compra: {factura_lenta['fecha_ces'].date() if factura_lenta['fecha_ces'] else 'N/A'}")
    print(f"- Emisión: {factura_lenta['fecha_emision'].date()}")
    print(f"- Pago: {factura_lenta['fecha_pago'].date()}")
    print(f"- Plazo: {factura_lenta['plazo']} días")

    # -------------------------------
    # 🟥 Sección adicional: MOROSOS
    # -------------------------------
    morosos = list(docs.find({
        "RUT DEUDOR": rut_deudor,
        "ESTADO": "MOROSO"
    }))

    print(f"\n📌 Facturas morosas en 'docs': ({len(morosos)} encontradas)")
    if not morosos:
        print("- No hay facturas con estado MOROSO.")
    else:
        for m in morosos:
            emision = parse_fecha(m.get("FEC EMISION DIG"))
            cesion = parse_fecha(m.get("FECHA CES"))
            monto = m.get("MONTO DOC")
            saldo = m.get("SALDO")
            dias_vencido = (datetime.today() - emision).days if emision else "N/A"

            print(f"- Monto: {monto:,.0f} | Saldo: {saldo:,.0f} | Compra: {cesion.date() if cesion else 'N/A'} | "
                  f"Emisión: {emision.date() if emision else 'N/A'} | Días desde la emisión: {dias_vencido}")

