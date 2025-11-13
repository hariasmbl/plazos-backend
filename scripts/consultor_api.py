from fastapi import FastAPI, Query, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import numpy as np
import os
import shutil
from scripts.consultor import aplicar_reglas_verano, obtener_tipo_entidad  # ✅ Integración con consultor.py

def normalizar_clave(n_doc, n_ope):
    """
    Normaliza claves de documentos/pagos para evitar inconsistencias
    Ej: diferencias entre "Nº DCTO", "Nª Doc.", espacios, mayúsculas, etc.
    """
    if n_doc is None or n_ope is None:
        return None
    return (str(n_doc).strip(), str(n_ope).strip())


UPLOAD_FOLDER = "data"

# ---------------------------------------
# 🔧 Conexión MongoDB
# ---------------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.server_info()
    print("✅ Conexión con MongoDB Atlas OK")
except Exception as e:
    print("❌ Error al conectar con MongoDB:", e)
    raise e

db = client["mi_base_datos"]
docs = db["docs"]
pagos = db["pagos"]
empresas_chile = db["empresas"]

# ---------------------------------------
# ⚙️ Configuración FastAPI
# ---------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://plazos-bl.web.app",
        "https://plazos-bl.firebaseapp.com",
        "http://localhost:5173",  # para pruebas locales con Vite o similar
        "http://localhost:5000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "ok"}

# ---------------------------------------
# 🧩 Funciones auxiliares
# ---------------------------------------
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

# ---------------------------------------
# 🔍 CONSULTAR RUT
# ---------------------------------------
@app.get("/consultar-rut")
def consultar_por_rut(rut: str = Query(..., alias="rut")):
    facturas = list(docs.find({"RUT DEUDOR": rut}))
    pagos_deudor = list(pagos.find({"Rut Deudor": rut}))
    pagos_dict = {
        normalizar_clave(p.get("Nª Doc."), p.get("Nº Ope.")): p
        for p in pagos_deudor
        if p.get("Estado") == "PAGADO"
    }


    registros_validos = []
    for f in facturas:
        clave = normalizar_clave(f.get("Nº DCTO"), f.get("Nº OPE"))
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

    # ======================================================
    # === CASO 1: RUT CON HISTORIAL DE PAGOS ===============
    # ======================================================
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

        # -------------------------------
        # Integración con reglas de verano
        # -------------------------------
        from scripts.consultor import aplicar_reglas_verano

        # Promedio solo en meses de verano
        registros_verano = [r for r in registros_limpios if r["fecha_pago"].month in [11, 12, 1, 2]]
        promedio_verano = np.mean([r["plazo"] for r in registros_verano]) if registros_verano else np.nan
        desviacion_verano = np.std([r["plazo"] for r in registros_verano]) if registros_verano else None

        # Aplicar reglas con desviación incluida

        reglas = aplicar_reglas_verano(rut, promedio_verano, promedio, desviacion_verano, desviacion)
        plazo_regla = reglas.get("plazo_recomendado")
        factor_dias = reglas.get("factor_dias", 15)
        tipo = reglas.get("tipo") or "NORMAL"

        # 🔧 Validación adicional para casos normales
        if tipo == "NORMAL" and factor_dias == 7.5:
            factor_dias = 15

        if plazo_regla and not np.isnan(plazo_regla):
            plazo_recomendado = plazo_regla

        # -------------------------------
        # Facturas morosas
        # -------------------------------
        morosos = list(docs.find({"RUT DEUDOR": rut, "ESTADO": "MOROSO"}))
        facturas_morosas = []
        for m in morosos:
            clave_m = normalizar_clave(m.get("Nº DCTO"), m.get("Nº OPE"))

            claves_pagadas = {
                normalizar_clave(f.get("Nº DCTO"), f.get("Nº OPE"))
                for f in facturas
                if normalizar_clave(f.get("Nº DCTO"), f.get("Nº OPE")) in pagos_dict
            }

            if clave_m in claves_pagadas:
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

        # -------------------------------
        # 🔧 Evitar errores por NaN o valores no serializables
        # -------------------------------
        def safe_num(x):
            try:
                if x is None or (isinstance(x, float) and np.isnan(x)):
                    return 0
                return float(x)
            except Exception:
                return 0

        promedio_ultimos = safe_num(promedio_ultimos)
        promedio = safe_num(promedio)
        desviacion = safe_num(desviacion)
        plazo_recomendado = safe_num(plazo_recomendado)

        # -------------------------------
        # Retorno final
        # -------------------------------
        return {
            "nombre_deudor": facturas[0].get("DEUDOR", "Desconocido"),
            "tipo_entidad": tipo,
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
            "promedio_historico": promedio,
            "desviacion_estandar": desviacion,
            "cantidad_historico": len(registros_limpios),
            "factura_mas_lenta": factura_lenta,
            "plazo_recomendado": plazo_recomendado,
            "factor_dias": factor_dias,
            "recomendacion": recomendacion,
            "morosos": facturas_morosas,
            "riesgo_detectado": hay_riesgo
        }


    # ======================================================
    # === CASO 2: RUT SIN HISTORIAL ========================
    # ======================================================
    empresa = empresas_chile.find_one({"rut": rut})
    if not empresa:
        return {
            "error": "RUT no tiene historial ni está registrado en la base de empresas.",
            "plazo_recomendado": 30,
            "recomendacion": "No existe información histórica. Se recomienda usar un plazo base de 30 días."
        }

    rubro = empresa.get("rubro")
    tramo = empresa.get("tramo_ventas")

    if not rubro or not tramo:
        return {
            "nombre_deudor": empresa.get("nombre", "Desconocido"),
            "error": "Faltan datos de rubro o tramo de ventas.",
            "recomendacion": "No es posible estimar plazo por similitud. Utilice el plazo base de 30 días.",
            "plazo_recomendado": 30,
            "ultimos_pagos": [],
            "morosos": [],
            "empresas_similares": False
        }

    # --- Buscar empresas similares ---
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
        return {
            "nombre_deudor": empresa.get("nombre", "Desconocido"),
            "error": "No se encontraron pagos de empresas similares.",
            "recomendacion": "No es posible estimar plazo. Se recomienda usar 30 días como base.",
            "plazo_recomendado": 30,
            "ultimos_pagos": [],
            "morosos": [],
            "empresas_similares": False
        }

    # --- Cálculo para empresas similares ---
    promedio_bruto = np.mean(plazos_similares)
    desviacion_bruto = np.std(plazos_similares)
    plazos_limpios = [p for p in plazos_similares if not es_outlier(p, promedio_bruto, desviacion_bruto)]

    if not plazos_limpios:
        return {
            "nombre_deudor": empresa.get("nombre", "Desconocido"),
            "error": "Datos insuficientes para estimar plazo confiable.",
            "recomendacion": "Se recomienda usar 30 días como base.",
            "plazo_recomendado": 30,
            "empresas_similares": False
        }

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
        "empresas_similares": True
    }

# ---------------------------------------
# 📂 Subida de archivos
# ---------------------------------------
@app.post("/subir-docs")
async def subir_docs(file: UploadFile = File(...)):
    return await guardar_archivo(file, "list docs")

@app.post("/subir-pagos")
async def subir_pagos(file: UploadFile = File(...)):
    return await guardar_archivo(file, "cartola")

@app.post("/subir-empresas")
async def subir_empresas(file: UploadFile = File(...)):
    return await guardar_archivo(file, "empresas")

async def guardar_archivo(file: UploadFile, tipo: str):
    try:
        filename = file.filename
        ruta = os.path.join(UPLOAD_FOLDER, filename)
        with open(ruta, "wb") as f:
            shutil.copyfileobj(file.file, f)

        if tipo == "list docs":
            from scripts.cargar_datos import cargar_excel, insertar_documentos
            df = cargar_excel(ruta)
            if not df.empty:
                resumen = insertar_documentos(df, filename)
                return {"mensaje": f"✅ Archivo {filename} subido y procesado.", "resumen": resumen}

        elif tipo == "cartola":
            from scripts.cargar_pagos import cargar_y_limpiar_excel, insertar_documentos
            df = cargar_y_limpiar_excel(ruta)
            if not df.empty:
                resumen = insertar_documentos(df, filename)
                return {"mensaje": f"✅ Archivo {filename} subido y procesado.", "resumen": resumen}

        elif tipo == "empresas":
            from scripts.cargar_empresas import procesar_txt
            procesar_txt(ruta)
            return {"mensaje": f"✅ Archivo {filename} subido y empresas actualizadas."}

        return {"mensaje": f"⚠️ El archivo {filename} no contenía datos válidos."}
    except Exception as e:
        return {"mensaje": f"❌ Error al subir archivo: {str(e)}"}
