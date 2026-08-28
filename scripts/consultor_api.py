from fastapi import FastAPI, Query, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from collections import defaultdict
from pydantic import BaseModel
import numpy as np
import os
import shutil
import io
import re

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
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


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


# ============================================================
# 🔧 Normalización y validación de RUT
# ============================================================

RUT_RE = re.compile(r"^\d{6,8}(-[0-9K])?$")

# Nombres de columna que sugieren claramente que NO es una columna de RUT,
# aunque su contenido tenga la misma forma (dígitos) que un RUT sin DV.
NOMBRES_NO_RUT = re.compile(
    r"documento|folio|factura|boleta|n[uú]mero|n[uú]m\b|\bid\b|c[oó]digo|monto|fecha|total|orden|guia",
    re.IGNORECASE
)


def parece_rut(valor):
    """True si el valor, limpio de puntos/espacios, tiene forma de RUT chileno
    (6 a 8 dígitos, con o sin guion+DV). Es una heurística de contenido, no
    una validación real — se usa para detectar en qué columna de un archivo
    subido probablemente vienen los RUTs."""
    limpio = str(valor).strip().replace(".", "").replace(" ", "").upper()
    return bool(RUT_RE.match(limpio))


def calcular_dv(rut_numero):
    """Dígito verificador chileno (módulo 11) para la parte numérica de un RUT."""
    suma = 0
    multiplicador = 2
    for digito in reversed(str(rut_numero)):
        suma += int(digito) * multiplicador
        multiplicador = multiplicador + 1 if multiplicador < 7 else 2
    resto = 11 - (suma % 11)
    if resto == 11:
        return "0"
    if resto == 10:
        return "K"
    return str(resto)


def normalizar_y_completar_rut(valor):
    """
    Limpia un RUT (quita puntos/espacios, mayúsculas) y completa el dígito
    verificador si falta, calculándolo con módulo 11. Si ya trae DV, lo
    valida contra el cálculo pero NO lo corrige si no calza — podría ser un
    RUT mal tipeado por completo, no solo el DV — solo lo marca.

    Devuelve (rut_final, estado):
    - "ok": no hizo falta tocarlo, DV correcto.
    - "completado": no traía DV, se calculó y agregó.
    - "dv_sospechoso": traía DV pero no coincide con el cálculo esperado
      (se deja tal cual vino, sin corregir).
    - "invalido": no se pudo interpretar como RUT (rut_final es None).
    """
    limpio = str(valor).strip().replace(".", "").replace(" ", "").upper()
    if not limpio:
        return None, "invalido"

    if "-" in limpio:
        numero, dv = limpio.rsplit("-", 1)
    else:
        numero, dv = limpio, None

    if not numero.isdigit() or not (1 <= len(numero) <= 8):
        return None, "invalido"

    dv_calculado = calcular_dv(numero)

    if dv is None:
        return f"{numero}-{dv_calculado}", "completado"

    if dv != dv_calculado:
        return f"{numero}-{dv}", "dv_sospechoso"

    return f"{numero}-{dv}", "ok"


def _cruzar(facturas, pagos_deudor, rut=None):
    """
    Núcleo puro del cruce factura-pago: recibe las listas ya obtenidas de
    Mongo (para poder reusarlo tanto con una query por RUT como con un
    batch de varios RUTs pre-cargados con $in) y hace todo el cálculo en
    memoria. Devuelve (pagos_dict, registros_validos, registros_limpios,
    promedio, desviacion) — mismo criterio de filtrado que antes.
    """
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
        return pagos_dict, [], [], None, None

    plazos = [r["plazo"] for r in registros_validos]
    promedio = np.mean(plazos)
    desviacion = np.std(plazos)

    registros_limpios = [
        r for r in registros_validos
        if not es_outlier(r["plazo"], promedio, desviacion)
    ]

    return pagos_dict, registros_validos, registros_limpios, promedio, desviacion


def cruzar_facturas_pagos(rut):
    """
    Cruza facturas y pagos de un RUT deudor (consulta individual).
    Devuelve (facturas, pagos_dict, registros_validos, registros_limpios, promedio, desviacion),
    donde registros_validos excluye plazos anómalos (<0 o >300 días) y
    registros_limpios además excluye outliers (z-score > 2 sobre registros_validos).
    """
    facturas = list(docs.find({"RUT DEUDOR": rut}))
    pagos_deudor = list(pagos.find({"Rut Deudor": rut}))
    pagos_dict, registros_validos, registros_limpios, promedio, desviacion = _cruzar(facturas, pagos_deudor, rut)
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

def procesar_rut(rut, facturas, pagos_deudor, morosos):
    """
    Misma lógica de negocio que antes tenía consultar_por_rut(), pero
    recibiendo facturas/pagos/morosos ya obtenidos de Mongo en vez de
    consultarlos ella misma — así /consultar-rut (una query) y
    /consultar-ruts-batch (una query $in para todos los RUTs) pueden
    compartir exactamente la misma función.
    """
    pagos_dict, registros_validos, registros_limpios, promedio, desviacion = _cruzar(facturas, pagos_deudor, rut)

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


@app.get("/consultar-rut")
def consultar_por_rut(rut: str = Query(..., alias="rut")):
    rut_final, estado = normalizar_y_completar_rut(rut)

    if estado == "invalido":
        return {"error": "RUT inválido — verifica el formato (ej: 12345678-9)."}

    if estado == "dv_sospechoso":
        return {"error": "DV inválido — revisa el RUT.", "dv_sospechoso": True, "rut": rut_final}

    facturas = list(docs.find({"RUT DEUDOR": rut_final}))
    pagos_deudor = list(pagos.find({"Rut Deudor": rut_final}))
    morosos = list(docs.find({"RUT DEUDOR": rut_final, "ESTADO": "MOROSO"}))
    resultado = procesar_rut(rut_final, facturas, pagos_deudor, morosos)
    resultado["rut"] = rut_final
    return resultado


# ============================================================
# 🔍 CONSULTAR MÚLTIPLES RUT (batch)
# ============================================================

LIMITE_BATCH_RUTS = 50


class RutsBatchRequest(BaseModel):
    ruts: list[str]


@app.post("/consultar-ruts-batch")
def consultar_ruts_batch(payload: RutsBatchRequest):
    completados = 0
    dv_sospechoso = 0
    invalidos = 0
    ruts_a_consultar = []
    resultados_dv_sospechoso = []

    for valor in payload.ruts:
        if not valor or not valor.strip():
            continue
        rut_final, estado = normalizar_y_completar_rut(valor)
        if estado == "invalido":
            invalidos += 1
            continue
        if estado == "dv_sospechoso":
            # No se consulta: el DV no calza con el cálculo esperado, así
            # que casi seguro no va a matchear nada en Mongo -- y mostrar
            # eso como "sin historial" escondería que el problema real es
            # que el RUT está mal escrito.
            dv_sospechoso += 1
            resultados_dv_sospechoso.append({
                "rut": rut_final,
                "error": "DV inválido — revisa el RUT",
                "dv_sospechoso": True
            })
            continue
        if estado == "completado":
            completados += 1
        ruts_a_consultar.append(rut_final)

    # Dedup preservando el orden de entrada
    ruts = list(dict.fromkeys(ruts_a_consultar))

    resumen_normalizacion = {
        "completados": completados,
        "dv_sospechoso": dv_sospechoso,
        "invalidos": invalidos
    }

    if not ruts and not resultados_dv_sospechoso:
        return JSONResponse(status_code=400, content={"error": "Debe incluir al menos un RUT válido."})

    if len(ruts) > LIMITE_BATCH_RUTS:
        return JSONResponse(
            status_code=400,
            content={"error": f"Máximo {LIMITE_BATCH_RUTS} RUTs por consulta (se recibieron {len(ruts)})."}
        )

    resultados = list(resultados_dv_sospechoso)

    if ruts:
        # Una sola query $in para todos los RUTs en vez de una por RUT
        facturas_por_rut = defaultdict(list)
        for d in docs.find({"RUT DEUDOR": {"$in": ruts}}):
            facturas_por_rut[d.get("RUT DEUDOR")].append(d)

        pagos_por_rut = defaultdict(list)
        for p in pagos.find({"Rut Deudor": {"$in": ruts}}):
            pagos_por_rut[p.get("Rut Deudor")].append(p)

        morosos_por_rut = defaultdict(list)
        for m in docs.find({"RUT DEUDOR": {"$in": ruts}, "ESTADO": "MOROSO"}):
            morosos_por_rut[m.get("RUT DEUDOR")].append(m)

        for rut in ruts:
            try:
                resultado = procesar_rut(
                    rut,
                    facturas_por_rut.get(rut, []),
                    pagos_por_rut.get(rut, []),
                    morosos_por_rut.get(rut, [])
                )
            except Exception as e:
                resultado = {"error": f"Error al procesar RUT: {str(e)}"}

            resultado["rut"] = rut
            resultados.append(resultado)

    return {"resultados": resultados, "resumen_normalizacion": resumen_normalizacion}


def _valor_parece_dato(valor):
    """
    True si el valor se ve como un dato (numérico, RUT o fecha) en vez de
    un título de columna descriptivo (ej. "Nombre Cliente", "RUT").
    """
    limpio = str(valor).strip()
    if not limpio:
        return False
    if parece_rut(limpio):
        return True
    try:
        float(limpio.replace(",", "."))
        return True
    except ValueError:
        pass
    if re.match(r"^\d{1,4}[-/]\d{1,2}[-/]\d{1,4}$", limpio):
        return True
    return False


def _tiene_header(df_sin_header):
    """
    Heurística: compara la fila 0 contra el resto de cada columna. Si en la
    mayoría de las columnas la fila 0 se ve como texto descriptivo mientras
    el resto son datos (numéricos/RUT/fecha), es un header real. Si la fila
    0 "encaja" con el patrón de datos del resto, probablemente no hay header
    y pandas se tragó una fila de datos como nombre de columna.
    """
    if len(df_sin_header) < 2:
        return True  # muy pocas filas para comparar con confianza

    fila0 = df_sin_header.iloc[0]
    resto = df_sin_header.iloc[1:]

    columnas_evaluables = 0
    columnas_con_contraste = 0

    for col in df_sin_header.columns:
        valores_resto = [v for v in resto[col].tolist() if str(v).strip() and str(v).strip().lower() != "nan"]
        if not valores_resto:
            continue

        columnas_evaluables += 1
        resto_son_datos = sum(1 for v in valores_resto if _valor_parece_dato(v)) / len(valores_resto) > 0.6
        header_es_texto = not _valor_parece_dato(fila0[col])

        if resto_son_datos and header_es_texto:
            columnas_con_contraste += 1

    if columnas_evaluables == 0:
        return True

    return (columnas_con_contraste / columnas_evaluables) > 0.5


@app.post("/extraer-ruts")
async def extraer_ruts(file: UploadFile = File(...)):
    """
    Lee un Excel/CSV subido y devuelve la lista de RUTs, ya normalizados
    (DV completado si faltaba). Detecta si el archivo trae fila de
    encabezado o no, y si no encuentra una columna llamada "RUT", identifica
    la columna correcta por contenido (qué tan seguido matchea el patrón de
    RUT chileno) en vez de asumir la primera columna a ciegas.
    """
    import pandas as pd

    try:
        contenido = await file.read()
        nombre = (file.filename or "").lower()

        def leer(header):
            if nombre.endswith(".csv"):
                return pd.read_csv(io.BytesIO(contenido), header=header, dtype=str)
            return pd.read_excel(io.BytesIO(contenido), header=header, dtype=str)

        df_crudo = leer(header=None)

        if df_crudo.empty or len(df_crudo.columns) == 0:
            return JSONResponse(status_code=400, content={"error": "El archivo no tiene columnas legibles."})

        hay_header = _tiene_header(df_crudo)
        df = leer(header=0) if hay_header else df_crudo

        columna_rut = next(
            (col for col in df.columns if str(col).strip().upper() == "RUT"),
            None
        )

        if columna_rut is None:
            # Si el archivo tiene encabezado real, un nombre de columna que
            # claramente sugiere "no es RUT" (ej. "NumDocumento", "Folio")
            # pesa más que el simple parecido de forma con un RUT sin DV --
            # de lo contrario cualquier número de 6-8 dígitos se confundiría
            # con un RUT. Sin encabezado, no hay nombre que lo desmienta.
            columnas_candidatas = [
                col for col in df.columns
                if not (hay_header and NOMBRES_NO_RUT.search(str(col)))
            ]

            UMBRAL_SCORE_RUT = 0.7
            MARGEN_EMPATE = 0.2
            orden_columnas = list(df.columns)

            scores = {}
            for col in columnas_candidatas:
                valores = [v for v in df[col].tolist() if str(v).strip() and str(v).strip().lower() != "nan"]
                if not valores:
                    continue
                scores[col] = sum(1 for v in valores if parece_rut(v)) / len(valores)

            candidatos_validos = [col for col, s in scores.items() if s >= UMBRAL_SCORE_RUT]

            # Si ninguna columna alcanza el umbral, seguimos sin adivinar.
            if not candidatos_validos:
                return JSONResponse(status_code=400, content={
                    "error": "No se pudo identificar con confianza la columna de RUTs. "
                             "Verifica que el archivo tenga una columna llamada \"RUT\", o que los "
                             "RUTs sean claramente reconocibles (sin mezclarse con otros números)."
                })

            # Entre las candidatas que sí alcanzan el umbral, si varias
            # empatan (dentro del margen respecto a la mejor), preferimos la
            # más a la izquierda: los archivos que suben los usuarios
            # siempre traen el RUT como primera columna, así que la
            # posición es una señal confiable para desempatar.
            mejor_score = max(scores[col] for col in candidatos_validos)
            empatadas = [col for col in candidatos_validos if scores[col] >= mejor_score - MARGEN_EMPATE]
            columna_rut = min(empatadas, key=lambda col: orden_columnas.index(col))

        valores_originales = [str(v).strip() for v in df[columna_rut].tolist() if str(v).strip() and str(v).strip().lower() != "nan"]

        ruts, completados, dv_sospechoso, invalidos = [], 0, 0, 0
        for valor in valores_originales:
            rut_final, estado = normalizar_y_completar_rut(valor)
            if estado == "invalido":
                invalidos += 1
                continue
            if estado == "completado":
                completados += 1
            elif estado == "dv_sospechoso":
                dv_sospechoso += 1
            ruts.append(rut_final)

        return {
            "ruts": ruts,
            "columna_usada": str(columna_rut),
            "resumen_normalizacion": {
                "completados": completados,
                "dv_sospechoso": dv_sospechoso,
                "invalidos": invalidos
            }
        }

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"No se pudo leer el archivo: {str(e)}"})


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

def actualizar_estado_carga(tipo, estado, mensaje=None, tocar_fecha=False, **extra):
    campos = {"tipo": tipo, "estado": estado, "mensaje": mensaje, **extra}
    if tocar_fecha:
        campos["ultima_actualizacion"] = datetime.now()

    if estado in ("listo", "error"):
        registro = db["metadata"].find_one({"tipo": tipo})
        inicio = registro.get("inicio") if registro else None
        if inicio:
            campos["duracion_segundos"] = (datetime.now() - inicio).total_seconds()

    db["metadata"].update_one({"tipo": tipo}, {"$set": campos}, upsert=True)


def formatear_resumen(resumen):
    partes = [f"{resumen.get('nuevos', 0)} nuevos"]
    if "duplicados" in resumen:
        partes.append(f"{resumen['duplicados']} duplicados")
    if resumen.get("actualizados"):
        partes.append(f"{resumen['actualizados']} actualizados")
    return ", ".join(partes)


# ------------------------------------------------------------
# Las 3 cargas (docs, pagos, empresas) se procesan en segundo
# plano: dependiendo de la cantidad de filas, insertar en Mongo
# fila por fila puede tardar minutos, y al ser sincrono dentro
# de un endpoint async bloqueaba TODO el servidor (un solo
# worker) mientras procesaba. Con BackgroundTasks la subida del
# archivo responde de inmediato y el procesamiento corre en un
# hilo aparte; el frontend consulta /estado-carga por el avance.
# ------------------------------------------------------------

def procesar_docs_background(ruta, filename):
    from scripts.cargar_datos import cargar_excel, insertar_documentos
    try:
        df = cargar_excel(ruta)
        if df.empty:
            actualizar_estado_carga("docs", "error", mensaje="Archivo sin datos válidos")
            return
        resumen = insertar_documentos(df, filename)
        actualizar_estado_carga("docs", "listo", mensaje=formatear_resumen(resumen), tocar_fecha=True)
    except Exception as e:
        actualizar_estado_carga("docs", "error", mensaje=str(e))
    finally:
        if os.path.exists(ruta):
            os.remove(ruta)


def procesar_pagos_background(ruta, filename):
    from scripts.cargar_pagos import cargar_y_limpiar_excel, insertar_documentos
    try:
        df = cargar_y_limpiar_excel(ruta)
        if df.empty:
            actualizar_estado_carga("pagos", "error", mensaje="Archivo sin datos válidos")
            return
        resumen = insertar_documentos(df, filename)
        actualizar_estado_carga("pagos", "listo", mensaje=formatear_resumen(resumen), tocar_fecha=True)
    except Exception as e:
        actualizar_estado_carga("pagos", "error", mensaje=str(e))
    finally:
        if os.path.exists(ruta):
            os.remove(ruta)


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


def recibir_archivo(background_tasks, file, tipo, funcion_background, *args_extra):
    try:
        filename = file.filename
        ruta = os.path.join(UPLOAD_FOLDER, filename)

        with open(ruta, "wb") as f:
            shutil.copyfileobj(file.file, f)

        peso_bytes = os.path.getsize(ruta)
        actualizar_estado_carga(
            tipo, "procesando",
            archivo=filename, peso_bytes=peso_bytes, inicio=datetime.now()
        )
        background_tasks.add_task(funcion_background, ruta, *args_extra)

        return {"mensaje": f"Archivo {filename} recibido, procesando en segundo plano."}

    except Exception as e:
        actualizar_estado_carga(tipo, "error", mensaje=str(e))
        return JSONResponse(status_code=500, content={"mensaje": f"Error al subir archivo: {str(e)}"})


@app.post("/subir-docs")
async def subir_docs(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    return recibir_archivo(background_tasks, file, "docs", procesar_docs_background, file.filename)


@app.post("/subir-pagos")
async def subir_pagos(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    return recibir_archivo(background_tasks, file, "pagos", procesar_pagos_background, file.filename)


@app.post("/subir-empresas")
async def subir_empresas(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    return recibir_archivo(background_tasks, file, "empresas", procesar_empresas_background)


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
            "archivo": r.get("archivo"),
            "peso_bytes": r.get("peso_bytes"),
            "duracion_segundos": r.get("duracion_segundos"),
        }

    return {
        "docs": resumen("docs"),
        "pagos": resumen("pagos"),
        "empresas": resumen("empresas"),
    }
