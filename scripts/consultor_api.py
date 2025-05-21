from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import numpy as np
import os

# Configuración inicial
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["mi_base_datos"]
docs = db["docs"]
pagos = db["pagos"]
empresas_chile = db["empresas"]

app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "ok"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://plazos-bl.web.app"  # frontend en producción
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

tramos_uf = {
    "1": "Sin Información: no se puede estimar monto de ventas.",
    "2": "1er Rango Micro Empresa: 0,01 a 200,00 UF anuales",
    "3": "2do Rango Micro Empresa: 200,01 a 600,00 UF anuales",
    "4": "3er Rango Micro Empresa: 600,01 a 2.400,00 UF anuales",
    "5": "1er Rango Pequeña Empresa: 2.400,01 a 5.000,00 UF anuales",
    "6": "2do Rango Pequeña Empresa: 5.000,01 a 10.000,00 UF anuales",
    "7": "3er Rango Pequeña Empresa: 10.000,01 a 25.000,00 UF anuales",
    "8": "1er Rango Mediana Empresa: 25.000,01 a 50.000,00 UF anuales",
    "9": "2do Rango Mediana Empresa: 50.000,01 a 100.000,00 UF anuales",
    "10": "1er Rango Gran Empresa: 100.000,01 a 200.000,00 UF anuales",
    "11": "2do Rango Gran Empresa: 200.000,01 a 600.000,00 UF anuales",
    "12": "3er Rango Gran Empresa: 600.000,01 a 1.000.000,00 UF anuales",
    "13": "4to Rango Gran Empresa: más de 1.000.000,01 UF anuales"
}

@app.get("/consultar-rut")
def consultar_por_rut(rut: str = Query(..., alias="rut")):
    facturas = list(docs.find({"RUT DEUDOR": rut}))
    pagos_deudor = list(pagos.find({"Rut Deudor": rut}))
    pagos_dict = {(p.get("Nª Doc."), p.get("Nº Ope.")): p for p in pagos_deudor if p.get("Estado") == "PAGADO"}

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

    if registros_validos:
        plazos = [r["plazo"] for r in registros_validos]
        promedio = np.mean(plazos)
        desviacion = np.std(plazos)
        registros_limpios = [r for r in registros_validos if not es_outlier(r["plazo"], promedio, desviacion)]
        if not registros_limpios:
            return {"error": "Todos los registros fueron considerados outliers."}

        registros_limpios.sort(key=lambda x: x["fecha_pago"], reverse=True)
        ultimos_5 = registros_limpios[:5]
        promedio_ultimos = np.mean([r["plazo"] for r in ultimos_5])
        plazo_recomendado = max(30, round(promedio_ultimos + 0.5 * desviacion))

        morosos = list(docs.find({"RUT DEUDOR": rut, "ESTADO": "MOROSO"}))
        facturas_morosas = []
        for m in morosos:
            clave_m = (m.get("Nº DCTO"), m.get("Nº OPE"))
            if clave_m in pagos_dict:
                continue
            emision = parse_fecha(m.get("FEC EMISION DIG"))
            cesion = parse_fecha(m.get("FECHA CES"))
            vcto_nom = parse_fecha(m.get("VCTO NOM"))
            monto = m.get("MONTO DOC")
            saldo = m.get("SALDO")
            dias_vencido = (datetime.today() - emision).days if emision else None
            dias_mora = (datetime.today() - vcto_nom).days if vcto_nom else None
            facturas_morosas.append({
                "monto": monto,
                "saldo": saldo,
                "fecha_ces": cesion,
                "fecha_emision": emision,
                "dias_vencido": dias_vencido,
                "dias_mora": dias_mora
            })

        hay_riesgo = any(
            m.get("dias_vencido") and m["dias_vencido"] > plazo_recomendado
            for m in facturas_morosas
        )

        recomendacion = (
            "Hay documentos morosos que superan el plazo recomendado, revisar plazo y anticipo con riesgo"
            if hay_riesgo else
            f"Se recomienda cubrir {plazo_recomendado} días entre plazo y anticipo"
        )

        factura_lenta = max(registros_limpios, key=lambda x: x["plazo"])

        return {
            "nombre_deudor": facturas[0].get("DEUDOR", "Desconocido"),
            "ultimos_pagos": [
                {
                    "monto": r["monto"],
                    "fecha_ces": r["fecha_ces"],
                    "fecha_emision": r["fecha_emision"],
                    "fecha_pago": r["fecha_pago"],
                    "plazo": r["plazo"]
                } for r in ultimos_5
            ],
            "promedio_ultimos": promedio_ultimos,
            "promedio_historico": np.mean([r["plazo"] for r in registros_limpios]),
            "desviacion_estandar": desviacion,
            "cantidad_historico": len(registros_limpios),
            "factura_mas_lenta": factura_lenta,
            "plazo_recomendado": plazo_recomendado,
            "recomendacion": recomendacion,
            "morosos": facturas_morosas,
            "riesgo_detectado": hay_riesgo
        }

    # Si no hay historial, buscar en empresas_chile
    empresa = empresas_chile.find_one({"rut": rut})
    if not empresa:
        return {"error": "RUT no tiene historial ni está registrado en la base de empresas."}

    rubro = empresa.get("rubro")
    tramo = empresa.get("tramo_ventas")

    similares = list(empresas_chile.find({"rubro": rubro, "tramo_ventas": tramo}))
    ruts_similares = [e["rut"] for e in similares if "rut" in e]

    facturas_similares = list(docs.find({"RUT DEUDOR": {"$in": ruts_similares}}))
    pagos_similares = list(pagos.find({"Rut Deudor": {"$in": ruts_similares}, "Estado": "PAGADO"}))
    pagos_dict_sim = {(p.get("Nª Doc."), p.get("Nº Ope.")): p for p in pagos_similares}

    plazos_similares = []
    for f in facturas_similares:
        clave = (f.get("Nº DCTO"), f.get("Nº OPE"))
        pago = pagos_dict_sim.get(clave)
        if pago:
            fec_emision = parse_fecha(f.get("FEC EMISION DIG"))
            fec_pago = parse_fecha(pago.get("Fecha Pago"))
            if fec_emision and fec_pago:
                plazo = (fec_pago - fec_emision).days
                plazos_similares.append(plazo)

    if not plazos_similares:
        return {"error": "No hay datos de pago para empresas similares."}

    promedio_bruto = np.mean(plazos_similares)
    desviacion_bruto = np.std(plazos_similares)
    plazos_limpios = [p for p in plazos_similares if not es_outlier(p, promedio_bruto, desviacion_bruto)]

    if not plazos_limpios:
        return {"error": "No hay datos confiables (sin outliers) para empresas similares."}

    promedio = np.mean(plazos_limpios)
    desviacion = np.std(plazos_limpios)
    plazo_recomendado = max(30, round(promedio + 0.5 * desviacion))

    return {
        "nombre_deudor": empresa.get("nombre", "Desconocido"),
        "recomendacion": f"Se recomienda cubrir {plazo_recomendado} días entre plazo y anticipo",
        "rubro": rubro,
        "tramo": tramos_uf.get(tramo, f"Tramo desconocido ({tramo})"),
        "promedio_empresas_similares": promedio,
        "desviacion_empresas_similares": desviacion,
        "cantidad_empresas_similares": len(plazos_limpios),
        "plazo_recomendado": plazo_recomendado,
        "ultimos_pagos": [],
        "morosos": [],
        "empresas_similares": True  # ← Esta línea activa la sección en el frontend
    }
