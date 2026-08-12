from fastapi import FastAPI, Query, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import numpy as np
import os
import shutil

from scripts.consultor import aplicar_reglas_verano, obtener_tipo_entidad, normalizar_clave


# ============================================================
# 🔧 Conexión MongoDB
# ============================================================

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

UPLOAD_FOLDER = "data"


# ============================================================
# ⚙️ Configuración FastAPI
# ============================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://plazos-bl.web.app",
        "https://plazos-bl.firebaseapp.com",
        "http://localhost:5173",
        "http://localhost:5000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"status": "ok"}


# ============================================================
# 🧩 Funciones útiles
# ============================================================

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


def cruzar_facturas_pagos(rut):
    """
    Cruza facturas y pagos de un RUT deudor.
    Devuelve (facturas, pagos_dict, registros_validos, registros_limpios, promedio, desviacion),
    donde registros_validos excluye plazos anómalos (<0 o >300 días) y
    registros_limpios además excluye outliers (z-score > 2 sobre registros_validos).
    """
    facturas = list(docs.find({"RUT DEUDOR": rut}))
    pagos_deudor = list(pagos.find({"Rut Deudor": rut}))

    pagos_dict = {}
    for p in pagos_deudor:
        clave = normalizar_clave(p.get("Nª Doc."), p.get("Nº Ope."))
        if clave:
            pagos_dict[clave] = p

    registros_validos = []

    for f in facturas:
        clave_f = normalizar_clave(f.get("Nº DCTO"), f.get("Nº OPE"))

        if not clave_f:
            continue

        pago = pagos_dict.get(clave_f)

        if pago:
            fec_emision = parse_fecha(f.get("FEC EMISION DIG"))
            fec_pago = parse_fecha(pago.get("Fecha Pago"))
            fecha_ces = parse_fecha(f.get("FECHA CES"))
            monto = f.get("MONTO DOC")

            if fec_emision and fec_pago:
                plazo = (fec_pago - fec_emision).days

                # Filtro de plazos erróneos
                if plazo < 0 or plazo > 300:
                    print(
                        f"Ignorando plazo anómalo ({plazo} días) para RUT {rut} "
                        f"doc {f.get('Nº DCTO')} ope {f.get('Nº OPE')} "
                        f"(emisión={f.get('FEC EMISION DIG')}, pago={pago.get('Fecha Pago')})"
                    )
                    continue

                registros_validos.append({
                    "fecha_ces": fecha_ces,
                    "fecha_emision": fec_emision,
                    "fecha_pago": fec_pago,
                    "plazo": plazo,
                    "monto": monto,
                    "clave_normalizada": clave_f,
                    "clave_original": {
                        "factura_doc": f.get("Nº DCTO"),
                        "factura_ope": f.get("Nº OPE"),
                        "pago_doc": pago.get("Nª Doc."),
                        "pago_ope": pago.get("Nº Ope.")
                    }
                })

    if not registros_validos:
        return facturas, pagos_dict, [], [], None, None

    plazos = [r["plazo"] for r in registros_validos]
    promedio = np.mean(plazos)
    desviacion = np.std(plazos)

    registros_limpios = [
        r for r in registros_validos
        if not es_outlier(r["plazo"], promedio, desviacion)
    ]

    return facturas, pagos_dict, registros_validos, registros_limpios, promedio, desviacion


# ============================================================
# 🔍 DEBUG FORMATO DOC / OPE
# ============================================================

@app.get("/debug-format")
def debug_format(rut: str):

    facturas = list(docs.find({"RUT DEUDOR": rut}))
    pagos_deudor = list(pagos.find({"Rut Deudor": rut}))

    def limpiar_factura(f):
        return {
            "Nº DCTO": f.get("Nº DCTO"),
            "Nº OPE": f.get("Nº OPE")
        }

    def limpiar_pago(p):
        return {
            "Nª Doc.": p.get("Nª Doc."),
            "Nº Ope.": p.get("Nº Ope.")
        }

    return {
        "docs": [limpiar_factura(f) for f in facturas],
        "pagos": [limpiar_pago(p) for p in pagos_deudor]
    }


# ============================================================
# 🔍 CONSULTAR RUT
# ============================================================

@app.get("/consultar-rut")
def consultar_por_rut(rut: str = Query(..., alias="rut")):

    facturas, pagos_dict, registros_validos, registros_limpios, promedio, desviacion = cruzar_facturas_pagos(rut)

    if registros_validos:

        if not registros_limpios:
            return {"error": "Todos los registros fueron considerados outliers."}

        registros_limpios.sort(key=lambda x: x["fecha_pago"], reverse=True)
        ultimos_5 = registros_limpios[:5]

        promedio_ultimos = np.mean([r["plazo"] for r in ultimos_5])
        plazo_recomendado = max(30, round(promedio_ultimos + 0.5 * desviacion))

        registros_verano = [
            r for r in registros_limpios if r["fecha_pago"].month in [11, 12, 1, 2]
        ]

        promedio_verano = (
            np.mean([r["plazo"] for r in registros_verano]) if registros_verano else np.nan
        )
        desviacion_verano = (
            np.std([r["plazo"] for r in registros_verano]) if registros_verano else np.nan
        )

        reglas = aplicar_reglas_verano(
            rut, promedio_verano, promedio, desviacion_verano, desviacion
        )

        tipo = reglas.get("tipo") or "NORMAL"
        factor_dias = reglas.get("factor_dias", 15)
        plazo_regla = reglas.get("plazo_recomendado")

        # 🔧 Sobrescribir regla si es SERVIU/MINVU
        if tipo == "SERVIU / MINVU":
            plazo_recomendado = 180
            factor_dias = 7.5
        elif plazo_regla is not None and not np.isnan(plazo_regla):
            plazo_recomendado = plazo_regla

        morosos_data = []
        morosos = list(docs.find({"RUT DEUDOR": rut, "ESTADO": "MOROSO"}))

        claves_pagadas = set(
            normalizar_clave(f.get("Nº DCTO"), f.get("Nº OPE"))
            for f in facturas
            if normalizar_clave(f.get("Nº DCTO"), f.get("Nº OPE")) in pagos_dict
        )

        for m in morosos:
            clave_m = normalizar_clave(m.get("Nº DCTO"), m.get("Nº OPE"))
            if clave_m in claves_pagadas:
                continue

            emision = parse_fecha(m.get("FEC EMISION DIG"))
            cesion = parse_fecha(m.get("FECHA CES"))
            vcto = parse_fecha(m.get("VCTO NOM"))
            monto = m.get("MONTO DOC")
            saldo = m.get("SALDO")

            dias_vencido = (datetime.today() - emision).days if emision else None
            dias_mora = (datetime.today() - vcto).days if vcto else None

            morosos_data.append({
                "monto": monto,
                "saldo": saldo,
                "fecha_ces": cesion,
                "fecha_emision": emision,
                "dias_vencido": dias_vencido,
                "dias_mora": dias_mora
            })

        hay_riesgo = any(
            m["dias_vencido"] and m["dias_vencido"] > plazo_recomendado
            for m in morosos_data
        )

        recomendacion = (
            "Hay documentos morosos que superan el plazo recomendado, revisar plazo y anticipo con riesgo"
            if hay_riesgo else
            f"Se recomienda cubrir {plazo_recomendado} días entre plazo y anticipo"
        )

        factura_lenta = max(registros_limpios, key=lambda x: x["plazo"])

        return {
            "nombre_deudor": facturas[0].get("DEUDOR", "Desconocido"),
            "tipo_entidad": tipo,
            "ultimos_pagos": ultimos_5,
            "promedio_ultimos": float(promedio_ultimos),
            "promedio_historico": float(promedio),
            "desviacion_estandar": float(desviacion),
            "cantidad_historico": len(registros_limpios),
            "factura_mas_lenta": factura_lenta,
            "plazo_recomendado": float(plazo_recomendado),
            "factor_dias": factor_dias,
            "recomendacion": recomendacion,
            "morosos": morosos_data,
            "riesgo_detectado": hay_riesgo
        }
    
    # -------------------------------------------
    # municipalidades/corp SIN historial
    # -------------------------------------------
    tipo_entidad = obtener_tipo_entidad(rut)

    if tipo_entidad in ["MUNICIPALIDAD", "CORP MUNICIPAL", "SERVIU / MINVU"]:
        empresa_base = empresas_chile.find_one({"rut": rut})
        nombre = empresa_base.get("nombre") if empresa_base else "Entidad Pública (sin nombre registrado)"

        if tipo_entidad == "SERVIU / MINVU":
            plazo_recomendado = 180
            recomendacion = "Se recomienda cubrir 180 días entre plazo y anticipo (regla SERVIU/MINVU)."
        else:
            plazo_recomendado = 105
            recomendacion = "Se recomienda cubrir 105 días entre plazo y anticipo (promedio verano municipalidades)."

        return {
            "nombre_deudor": nombre,
            "tipo_entidad": tipo_entidad,
            "plazo_recomendado": plazo_recomendado,
            "factor_dias": 7.5,
            "ultimos_pagos": [],
            "morosos": [],
            "empresas_similares": False,
            "recomendacion": recomendacion
        }

    empresa = empresas_chile.find_one({"rut": rut})

    if not empresa:
        return {
            "error": "RUT no tiene historial ni está registrado en la base de empresas.",
            "plazo_recomendado": 30,
            "recomendacion": "No existe información histórica. Revisar empresas similares."
        }

    rubro = empresa.get("rubro")
    tramo = empresa.get("tramo_ventas")

    similares = list(empresas_chile.find({"rubro": rubro, "tramo_ventas": tramo}))
    ruts_similares = [e["rut"] for e in similares]

    facturas_sim = list(docs.find({"RUT DEUDOR": {"$in": ruts_similares}}))
    pagos_sim = list(pagos.find({"Rut Deudor": {"$in": ruts_similares}}))

    pagos_sim_dict = {
        normalizar_clave(p.get("Nª Doc."), p.get("Nº Ope.")): p
        for p in pagos_sim
    }

    plazos_sim = []
    for f in facturas_sim:
        clave = normalizar_clave(f.get("Nº DCTO"), f.get("Nº OPE"))
        pago = pagos_sim_dict.get(clave)
        if pago:
            fe = parse_fecha(f.get("FEC EMISION DIG"))
            fp = parse_fecha(pago.get("Fecha Pago"))
            if fe and fp:
                plazo = (fp - fe).days

                # filtro anti-basura
                if plazo < 0 or plazo > 365:
                    continue

                plazos_sim.append(plazo)

    if not plazos_sim:
        return {
            "nombre_deudor": empresa.get("nombre", "Desconocido"),
            "error": "No se encontraron pagos de empresas similares.",
            "plazo_recomendado": 30,
            "recomendacion": "Sin suficiente información. Plazo base 30 días."
        }

    promedio = np.mean(plazos_sim)
    desviacion = np.std(plazos_sim)
    plazo_recomendado = max(30, round(promedio + 0.5 * desviacion))

    return {
        "nombre_deudor": empresa.get("nombre", "Desconocido"),
        "recomendacion": f"Se recomienda cubrir {plazo_recomendado} días entre plazo y anticipo",
        "rubro": rubro,
        "tramo": tramo,
        "promedio_empresas_similares": float(promedio),
        "desviacion_empresas_similares": float(desviacion),
        "cantidad_empresas_similares": len(plazos_sim),
        "plazo_recomendado": plazo_recomendado,
        "ultimos_pagos": [],
        "morosos": [],
        "empresas_similares": True
    }


# ============================================================
# 📜 HISTÓRICO DE PAGOS (todos, no solo los últimos 5)
# ============================================================

@app.get("/historico-pagos")
def historico_pagos(rut: str = Query(..., alias="rut")):

    facturas, pagos_dict, registros_validos, registros_limpios, promedio, desviacion = cruzar_facturas_pagos(rut)

    if not facturas:
        return {"error": "No se encontraron documentos para este RUT.", "pagos": []}

    if not registros_limpios:
        return {
            "nombre_deudor": facturas[0].get("DEUDOR", "Desconocido"),
            "error": "No se encontraron pagos históricos válidos para este RUT.",
            "pagos": []
        }

    registros_limpios.sort(key=lambda x: x["fecha_pago"], reverse=True)

    return {
        "nombre_deudor": facturas[0].get("DEUDOR", "Desconocido"),
        "cantidad": len(registros_limpios),
        "pagos": registros_limpios
    }


# ============================================================
# 🔧 test-cruce
# ============================================================

@app.get("/test-cruce")
def test_cruce(rut: str):

    facturas = list(docs.find({"RUT DEUDOR": rut}))
    pagos_deudor = list(pagos.find({"Rut Deudor": rut}))

    pagos_dict = {}
    for p in pagos_deudor:
        clave = normalizar_clave(p.get("Nª Doc."), p.get("Nº Ope."))
        if clave:
            pagos_dict[clave] = p

    resultados = []

    for f in facturas:
        clave_f = normalizar_clave(f.get("Nº DCTO"), f.get("Nº OPE"))
        pago = pagos_dict.get(clave_f)

        resultados.append({
            "factura_raw": {
                "Nº DCTO": f.get("Nº DCTO"),
                "Nº OPE": f.get("Nº OPE")
            },
            "factura_normalizada": clave_f,
            "pago_encontrado": True if pago else False,
            "pago_raw": {
                "Nª Doc.": pago.get("Nª Doc.") if pago else None,
                "Nº Ope.": pago.get("Nº Ope.") if pago else None
            } if pago else None,
            "pago_normalizado": normalizar_clave(
                pago.get("Nª Doc.") if pago else None,
                pago.get("Nº Ope.") if pago else None
            ) if pago else None
        })

    return {
        "docs_encontrados": len(facturas),
        "pagos_encontrados": len(pagos_deudor),
        "cruces": resultados
    }


# ============================================================
# 🔧 test-pagos-keys
# ============================================================

@app.get("/test-pagos-keys")
def test_pagos_keys(rut: str = None):
    if rut:
        pagos_deudor = pagos.find({"Rut Deudor": rut})
    else:
        pagos_deudor = pagos.find().limit(20)

    claves = set()
    for p in pagos_deudor:
        for k in p.keys():
            claves.add(k)

    return {"claves_unicas_en_pagos": sorted(list(claves))}


# ============================================================
# 📂 Subida de archivos
# ============================================================

@app.post("/subir-docs")
async def subir_docs(file: UploadFile = File(...)):
    return await guardar_archivo(file, "list docs")


@app.post("/subir-pagos")
async def subir_pagos(file: UploadFile = File(...)):
    return await guardar_archivo(file, "cartola")


def actualizar_estado_carga(tipo, estado, mensaje=None, tocar_fecha=False):
    campos = {"tipo": tipo, "estado": estado, "mensaje": mensaje}
    if tocar_fecha:
        campos["ultima_actualizacion"] = datetime.now()
    db["metadata"].update_one({"tipo": tipo}, {"$set": campos}, upsert=True)


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
                actualizar_estado_carga("docs", "listo", tocar_fecha=True)
                return {"mensaje": f"Archivo {filename} procesado", "resumen": resumen}

        elif tipo == "cartola":
            from scripts.cargar_pagos import cargar_y_limpiar_excel, insertar_documentos
            df = cargar_y_limpiar_excel(ruta)
            if not df.empty:
                resumen = insertar_documentos(df, filename)
                actualizar_estado_carga("pagos", "listo", tocar_fecha=True)
                return {"mensaje": f"Archivo {filename} procesado", "resumen": resumen}

        return JSONResponse(status_code=400, content={"mensaje": "Archivo sin datos válidos"})

    except Exception as e:
        return JSONResponse(status_code=500, content={"mensaje": f"Error al subir archivo: {str(e)}"})


# ------------------------------------------------------------
# Empresas: archivo del SII (~1GB), se procesa en segundo plano
# para no dejar la request colgada ni bloquear otras consultas.
# ------------------------------------------------------------

def procesar_empresas_background(ruta):
    from scripts.cargar_empresas import procesar_txt
    try:
        total = procesar_txt(ruta)
        actualizar_estado_carga("empresas", "listo", mensaje=f"{total} empresas cargadas", tocar_fecha=True)
    except Exception as e:
        actualizar_estado_carga("empresas", "error", mensaje=str(e))
    finally:
        if os.path.exists(ruta):
            os.remove(ruta)


@app.post("/subir-empresas")
async def subir_empresas(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    try:
        filename = file.filename
        ruta = os.path.join(UPLOAD_FOLDER, filename)

        with open(ruta, "wb") as f:
            shutil.copyfileobj(file.file, f)

        actualizar_estado_carga("empresas", "procesando")
        background_tasks.add_task(procesar_empresas_background, ruta)

        return {"mensaje": f"Archivo {filename} recibido, procesando en segundo plano."}

    except Exception as e:
        actualizar_estado_carga("empresas", "error", mensaje=str(e))
        return JSONResponse(status_code=500, content={"mensaje": f"Error al subir archivo: {str(e)}"})


@app.get("/estado-carga")
def estado_carga():
    registros = {
        r["tipo"]: r
        for r in db["metadata"].find({"tipo": {"$in": ["docs", "pagos", "empresas"]}})
    }

    def resumen(tipo):
        r = registros.get(tipo, {})
        return {
            "estado": r.get("estado"),
            "mensaje": r.get("mensaje"),
            "ultima_actualizacion": r.get("ultima_actualizacion"),
        }

    return {
        "docs": resumen("docs"),
        "pagos": resumen("pagos"),
        "empresas": resumen("empresas"),
    }
