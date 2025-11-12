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
# Reglas especiales de verano
# -----------------------------

def obtener_clasificacion_bl():
    """
    Carga el archivo clasificacion_bl.xlsx desde la carpeta data/clasificaciones.
    """
    try:
        ruta_excel = r"C:\Users\Damsoft\Desktop\Plazos\data\clasificaciones\clasificacion_bl.xlsx"
        df = pd.read_excel(ruta_excel)
        df["RUT"] = df["RUT"].astype(str).str.strip().str.upper()
        df["CLASIFICACIÓN BL 2"] = df["CLASIFICACIÓN BL 2"].astype(str).str.upper()
        return df
    except Exception as e:
        print(f"⚠️ No se pudo leer clasificacion_bl.xlsx: {e}")
        return pd.DataFrame()

def tipo_entidad_por_rut(rut, df_bl):
    """
    Determina si el RUT pertenece al MOP, a una MUNICIPALIDAD o a una CORP MUNICIPAL.
    """
    rut = str(rut).strip().upper()
    if rut == "61202000-0":
        return "MOP"

    fila = df_bl.loc[df_bl["RUT"] == rut]
    if fila.empty:
        return None

    clasificacion = str(fila.iloc[0]["CLASIFICACIÓN BL 2"]).upper()
    if "MUNICIPALIDADES" in clasificacion:
        return "MUNICIPALIDAD"
    if "CORP MUNICIPAL" in clasificacion:
        return "CORP MUNICIPAL"

    return None

def aplicar_reglas_verano(rut, promedio_verano, promedio_anual):
    """
    Aplica las reglas especiales para Municipalidades, Corporaciones o MOP entre noviembre y febrero.
    """
    mes_actual = datetime.now().month
    df_bl = obtener_clasificacion_bl()
    tipo = tipo_entidad_por_rut(rut, df_bl)

    # Si no es un mes de verano o no aplica la clasificación, se usa el cálculo normal
    if mes_actual not in [11, 12, 1, 2] or tipo is None:
        return {"plazo_recomendado": None, "factor_dias": 15}

    # Reglas especiales de verano
    if tipo == "MOP":
        return {"plazo_recomendado": 60, "factor_dias": 7.5}

    # Si no hay pagos en verano, usamos promedio anual
    promedio = promedio_verano if not np.isnan(promedio_verano) else promedio_anual

    if promedio <= 45:
        return {"plazo_recomendado": 45, "factor_dias": 7.5}
    elif 46 <= promedio <= 70:
        return {"plazo_recomendado": 90, "factor_dias": 7.5}
    elif 71 <= promedio <= 90:
        return {"plazo_recomendado": 105, "factor_dias": 7.5}
    else:
        # Evaluación puntual: no se recomienda compra automática
        return {"plazo_recomendado": None, "factor_dias": 7.5}

# -----------------------------
# Función principal
# -----------------------------

def consultar_por_rut(rut_deudor):
    print(f"\n📋 Consultando información para RUT DEUDOR: {rut_deudor}")

    # Documentos del RUT
    facturas = list(docs.find({"RUT DEUDOR": rut_deudor}))
    if not facturas:
        print("❌ No se encontraron documentos para este RUT.")
        return

    # Pagos asociados
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
        print("⚠️ No se encontraron coincidencias entre documentos y pagos.")
        return

    # Estadísticas generales
    plazos = [r["plazo"] for r in registros_validos]
    promedio_total = np.mean(plazos)
    desviacion_total = np.std(plazos)
    registros_limpios = [r for r in registros_validos if not es_outlier(r["plazo"], promedio_total, desviacion_total)]

    if not registros_limpios:
        print("⚠️ Todos los registros fueron considerados outliers.")
        return

    registros_limpios.sort(key=lambda x: x["fecha_pago"], reverse=True)
    ultimos_5 = registros_limpios[:5]
    promedio_ultimos = np.mean([r["plazo"] for r in ultimos_5])
    factura_lenta = max(registros_limpios, key=lambda x: x["plazo"])

    # Pagos solo entre noviembre y febrero
    registros_verano = [r for r in registros_limpios if r["fecha_pago"].month in [11, 12, 1, 2]]
    promedio_verano = np.mean([r["plazo"] for r in registros_verano]) if registros_verano else np.nan

    # Aplicar reglas especiales si corresponde
    reglas = aplicar_reglas_verano(rut_deudor, promedio_verano, promedio_total)
    plazo_recomendado = reglas["plazo_recomendado"]
    factor_dias = reglas["factor_dias"]

    # Morosos
    morosos = list(docs.find({"RUT DEUDOR": rut_deudor, "ESTADO": "MOROSO"}))
    print(f"\n📌 Facturas morosas encontradas: {len(morosos)}")

    print(f"\n📊 Promedio histórico: {promedio_total:.2f} días | Desviación: {desviacion_total:.2f}")
    print(f"📆 Promedio verano: {promedio_verano:.2f} días | Reglas aplicadas: {plazo_recomendado} días, factor {factor_dias}")

    return {
        "plazo_recomendado": plazo_recomendado,
        "factor_dias": factor_dias,
        "promedio_historico": promedio_total,
        "promedio_verano": promedio_verano,
        "cantidad_historico": len(registros_limpios),
        "ultimos_pagos": ultimos_5,
        "morosos": morosos
    }
