from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import numpy as np
import os
import pandas as pd

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
# Clasificación desde Excel
# -----------------------------

def obtener_tipo_entidad(rut):
    rut = str(rut).replace(".", "").strip().upper()

    try:
        # 🔹 1. Detectar MOP directamente
        if rut in ["61202000-0", "61.202.000-0", "612020000"]:
            print("✅ Detectado MOP por RUT")
            return "MOP"

        # 🔹 2. Detectar municipalidades / corporaciones por nombre en Mongo
        doc = docs.find_one({"RUT DEUDOR": rut})
        if doc:
            nombre = str(doc.get("DEUDOR", "")).upper()
            if "MUNICIPALIDAD" in nombre:
                print(f"✅ Detectada MUNICIPALIDAD por nombre: {nombre[:50]}")
                return "MUNICIPALIDAD"
            if "CORP" in nombre and "MUNICIPAL" in nombre:
                print(f"✅ Detectada CORP MUNICIPAL por nombre: {nombre[:50]}")
                return "CORP MUNICIPAL"

        # 🔹 3. Intentar leer Excel solo si existe
        base_dir = os.path.dirname(os.path.abspath(__file__))
        ruta_excel = os.path.join(base_dir, "..", "data", "clasificaciones", "clasificacion_bl.xlsx")

        if os.path.exists(ruta_excel):
            df = pd.read_excel(ruta_excel)
            df["RUT"] = df["RUT"].astype(str).str.replace(".", "", regex=False).str.strip().str.upper()
            fila = df.loc[df["RUT"] == rut]
            if not fila.empty:
                clasif = str(fila.iloc[0].get("CLASIFICACIÓN BL 2", "")).upper()
                if "MUNICIPALIDAD" in clasif:
                    return "MUNICIPALIDAD"
                elif "CORP" in clasif:
                    return "CORP MUNICIPAL"
        else:
            print(f"⚠️ Archivo clasificacion_bl.xlsx no encontrado en {ruta_excel}")

    except Exception as e:
        print(f"⚠️ Error al detectar tipo de entidad ({rut}): {e}")

    return None

# -----------------------------
# Reglas especiales de verano
# -----------------------------

def aplicar_reglas_verano(rut, promedio_verano, promedio_anual, desv_verano, desv_anual):
    mes = datetime.now().month
    tipo = obtener_tipo_entidad(rut)

    # Regla general (no es verano)
    if mes not in [11, 12, 1, 2]:
        return {
            "plazo_recomendado": None,
            "factor_dias": 15,  # 15 días por punto de anticipo = 2% mora
            "tipo": tipo
        }

    # Si no es muni / corp / MOP → reglas normales
    if tipo not in ["MUNICIPALIDAD", "CORP MUNICIPAL", "MOP"]:
        return {
            "plazo_recomendado": None,
            "factor_dias": 15,
            "tipo": tipo
        }

    # MOP
    if tipo == "MOP":
        return {
            "plazo_recomendado": 60,
            "factor_dias": 7.5,  # mora 4%
            "tipo": tipo
        }

    # Calcular promedio + desviación
    if not np.isnan(promedio_verano):
        base = promedio_verano + 0.5 * desv_verano
    else:
        base = promedio_anual + 0.5 * desv_anual

    # Reglas municipalidades / corporaciones
    if base <= 45:
        return {"plazo_recomendado": 45, "factor_dias": 7.5, "tipo": tipo}
    elif 46 <= base <= 70:
        return {"plazo_recomendado": 90, "factor_dias": 7.5, "tipo": tipo}
    elif 71 <= base <= 90:
        return {"plazo_recomendado": 105, "factor_dias": 7.5, "tipo": tipo}
    else:
        # >90 se evalúan puntualmente
        return {"plazo_recomendado": None, "factor_dias": 7.5, "tipo": tipo}

# -----------------------------
# Función principal
# -----------------------------

def consultar_por_rut(rut_deudor):
    print(f"\n📋 Consultando información para RUT DEUDOR: {rut_deudor}")

    facturas = list(docs.find({"RUT DEUDOR": rut_deudor}))

    if not facturas:
        print("❌ No se encontraron documentos para este RUT.")
        return {
            "error": "No se encontraron documentos para este deudor."
        }

    pagos_deudor = list(pagos.find({"Rut Deudor": rut_deudor}))
    pagos_dict = {(p.get("Nª Doc."), p.get("Nº Ope.")): p for p in pagos_deudor}

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
        return {
            "error": "No se encontraron coincidencias entre documentos y pagos."
        }

    # --- Filtrar outliers ---
    plazos = [r["plazo"] for r in registros_validos]
    promedio_total = np.mean(plazos)
    desviacion_total = np.std(plazos)

    registros_limpios = [
        r for r in registros_validos
        if not es_outlier(r["plazo"], promedio_total, desviacion_total)
    ]

    if not registros_limpios:
        return {"error": "Todos los registros fueron considerados outliers."}

    # Ordenar del pago más nuevo al más antiguo
    registros_limpios.sort(key=lambda x: x["fecha_pago"], reverse=True)

    # Últimos 5 pagos
    ultimos_5 = registros_limpios[:5]
    promedio_ultimos = np.mean([r["plazo"] for r in ultimos_5])

    # Factura más lenta
    factura_lenta = max(registros_limpios, key=lambda x: x["plazo"])

    # Registros de verano
    registros_verano = [
        r for r in registros_limpios
        if r["fecha_pago"].month in [11, 12, 1, 2]
    ]
    promedio_verano = np.mean([r["plazo"] for r in registros_verano]) if registros_verano else np.nan
    desviacion_verano = np.std([r["plazo"] for r in registros_verano]) if registros_verano else np.nan

    # Clasificar tipo de entidad
    reglas = aplicar_reglas_verano(
        rut_deudor,
        promedio_verano,
        promedio_total,
        desviacion_verano,
        desviacion_total
    )

    plazo_recomendado = reglas["plazo_recomendado"]
    factor_dias = reglas["factor_dias"]
    tipo = reglas["tipo"]

    # Morosos
    morosos = list(docs.find({"RUT DEUDOR": rut_deudor, "ESTADO": "MOROSO"}))

    # RESULTADO COMPATIBLE CON FRONTEND
    return {
        "nombre_deudor": facturas[0].get("DEUDOR", "Sin nombre"),
        "tipo_entidad": tipo,

        "plazo_recomendado": plazo_recomendado,
        "factor_dias": factor_dias,

        "ultimos_pagos": ultimos_5,
        "promedio_ultimos": float(promedio_ultimos),

        "cantidad_historico": len(registros_limpios),
        "promedio_historico": float(promedio_total),
        "desviacion_estandar": float(desviacion_total),

        "factura_mas_lenta": {
            "monto": factura_lenta["monto"],
            "fecha_ces": factura_lenta["fecha_ces"].strftime("%Y-%m-%d"),
            "fecha_emision": factura_lenta["fecha_emision"].strftime("%Y-%m-%d"),
            "fecha_pago": factura_lenta["fecha_pago"].strftime("%Y-%m-%d"),
            "plazo": factura_lenta["plazo"],
        },

        "morosos": [
            {
                "monto": m.get("MONTO DOC"),
                "saldo": m.get("SALDO", 0),
                "fecha_ces": m.get("FECHA CES"),
                "fecha_emision": m.get("FEC EMISION DIG"),
                "dias_vencido": m.get("DIAS DESDE EMISION", 0),
                "dias_mora": m.get("DIAS MORA", 0)
            } for m in morosos
        ],

        "recomendacion": (
            f"Se recomienda cubrir {plazo_recomendado} días entre plazo y anticipo."
            if plazo_recomendado else
            "Evaluación especial con riesgo."
        )
    }
