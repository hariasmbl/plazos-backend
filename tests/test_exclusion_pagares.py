"""
Tests de la exclusión de pagarés del cálculo de plazos (consultor_api).

Solo fixtures sintéticos: MongoClient y load_dotenv se reemplazan antes de
importar el módulo, así que no hay conexión a la base real. Las colecciones
se simulan en memoria con FakeCollection.

Correr desde la raíz del repo:  python -m pytest tests -q
"""
import sys
from datetime import datetime, timedelta
from unittest import mock

import numpy as np
import pytest

with mock.patch("pymongo.MongoClient"), mock.patch("dotenv.load_dotenv"):
    from scripts import consultor_api as api


RUT_DEUDOR = "76123456-0"
RUT_SOLO_PAGARES = "77777777-7"
RUT_SIMILAR = "76543210-3"
RUT_MUNI = "69010100-9"  # está en muni_ruts.txt

EMISION_BASE = datetime(2026, 3, 2)


class FakeCollection:
    """Lo mínimo de pymongo que usa procesar_rut: find/find_one con igualdad y $in."""

    def __init__(self, documentos):
        self.documentos = list(documentos)

    @staticmethod
    def _cumple(doc, query):
        for campo, cond in query.items():
            valor = doc.get(campo)
            if isinstance(cond, dict) and "$in" in cond:
                if valor not in cond["$in"]:
                    return False
            elif valor != cond:
                return False
        return True

    def find(self, query=None):
        return [d for d in self.documentos if self._cumple(d, query or {})]

    def find_one(self, query=None):
        encontrados = self.find(query)
        return encontrados[0] if encontrados else None


# ------------------------------------------------------------
# Constructores de fixtures
# ------------------------------------------------------------

def doc(rut, n_dcto, n_ope, tipo="FA", estado="PAGADO", emision=EMISION_BASE, monto=1_000_000):
    return {
        "RUT DEUDOR": rut,
        "DEUDOR": f"DEUDOR {rut}",
        "Nº DCTO": n_dcto,
        "Nº OPE": n_ope,
        "TIPO DCTO": tipo,
        "COD TIPO DOC": 4 if tipo == "PA" else 1,
        "ESTADO": estado,
        "FEC EMISION DIG": emision,
        "FECHA CES": emision + timedelta(days=1),
        "VCTO NOM": emision + timedelta(days=30),
        "MONTO DOC": monto,
        "SALDO": 0 if estado == "PAGADO" else monto,
    }


def pago(rut, n_doc, n_ope, fecha_pago, tipo_prod="FACTURA"):
    return {
        "Rut Deudor": rut,
        "Nª Doc.": float(n_doc),
        "Nº Ope.": float(n_ope),
        "Fecha Pago": fecha_pago.strftime("%d/%m/%Y"),
        "Tipo Prod.": tipo_prod,
    }


def historial(rut, plazos, tipo="FA", n_inicial=1, ope=500):
    """Un doc + un pago por plazo; cada pago 10 días después del anterior."""
    docs_, pagos_ = [], []
    tipo_prod = "PAGARÉ" if tipo == "PA" else "FACTURA"
    for i, plazo in enumerate(plazos):
        n = n_inicial + i
        emision = EMISION_BASE + timedelta(days=10 * i)
        docs_.append(doc(rut, n, ope, tipo=tipo, emision=emision))
        pagos_.append(pago(rut, n, ope, emision + timedelta(days=plazo), tipo_prod))
    return docs_, pagos_


def morosos_de(facturas):
    return [f for f in facturas if f["ESTADO"] == "MOROSO"]


@pytest.fixture
def colecciones(monkeypatch):
    """Instala colecciones falsas vacías; cada test agrega lo que necesita."""
    fake = {
        "docs": FakeCollection([]),
        "pagos": FakeCollection([]),
        "empresas": FakeCollection([]),
    }
    monkeypatch.setattr(api, "docs", fake["docs"])
    monkeypatch.setattr(api, "pagos", fake["pagos"])
    monkeypatch.setattr(api, "empresas_chile", fake["empresas"])
    return fake


def test_ruts_de_prueba_son_deudores_normales():
    # Si alguno quedara en las listas de entidades, cambiarían las reglas aplicadas.
    for rut in (RUT_DEUDOR, RUT_SOLO_PAGARES, RUT_SIMILAR):
        assert api.obtener_tipo_entidad(rut) is None
    assert api.obtener_tipo_entidad(RUT_MUNI) == "MUNICIPALIDAD"


# ------------------------------------------------------------
# Identificación de pagarés
# ------------------------------------------------------------

@pytest.mark.parametrize("valor", ["PAGARÉ", "PAGARE", " pagaré ", "Pagare"])
def test_identifica_pago_pagare(valor):
    assert api.es_pago_pagare({"Tipo Prod.": valor})


@pytest.mark.parametrize("valor", ["FACTURA", "FNC", None, ""])
def test_no_confunde_otros_pagos(valor):
    assert not api.es_pago_pagare({"Tipo Prod.": valor})


@pytest.mark.parametrize("valor,esperado", [("PA", True), (" pa ", True), ("FA", False), (None, False)])
def test_identifica_doc_pagare(valor, esperado):
    assert api.es_doc_pagare({"TIPO DCTO": valor}) is esperado


# ------------------------------------------------------------
# Deudor con facturas y pagarés mezclados
# ------------------------------------------------------------

PLAZOS_FA = [40, 42, 44, 46, 48, 50, 52]
# Un pagaré lento y uno rápido, con los pagos más recientes: si se colaran,
# moverían promedio, desviación, últimos 5 y factura de mayor plazo.
PLAZOS_PA = [150, 5]


def _mezclado():
    fa_docs, fa_pagos = historial(RUT_DEUDOR, PLAZOS_FA, tipo="FA", n_inicial=1)
    pa_docs, pa_pagos = historial(RUT_DEUDOR, PLAZOS_PA, tipo="PA", n_inicial=900, ope=777)
    # Pagarés pagados después que todas las facturas
    for d, p in zip(pa_docs, pa_pagos):
        d["FEC EMISION DIG"] += timedelta(days=200)
        fecha = datetime.strptime(p["Fecha Pago"], "%d/%m/%Y") + timedelta(days=200)
        p["Fecha Pago"] = fecha.strftime("%d/%m/%Y")
    return fa_docs, fa_pagos, pa_docs, pa_pagos


def test_mezclado_cifras_iguales_a_solo_facturas(colecciones):
    fa_docs, fa_pagos, pa_docs, pa_pagos = _mezclado()

    solo_fa = api.procesar_rut(RUT_DEUDOR, fa_docs, fa_pagos, [])
    mezclado = api.procesar_rut(RUT_DEUDOR, fa_docs + pa_docs, fa_pagos + pa_pagos, [])

    for campo in ("promedio_ultimos", "promedio_historico", "desviacion_estandar",
                  "cantidad_historico", "plazo_recomendado", "factor_dias",
                  "riesgo_detectado", "recomendacion"):
        assert mezclado[campo] == solo_fa[campo], campo
    assert mezclado["factura_mas_lenta"]["plazo"] == solo_fa["factura_mas_lenta"]["plazo"]
    assert [p["plazo"] for p in mezclado["ultimos_pagos"]] == [p["plazo"] for p in solo_fa["ultimos_pagos"]]


def test_mezclado_valores_esperados(colecciones):
    fa_docs, fa_pagos, pa_docs, pa_pagos = _mezclado()
    r = api.procesar_rut(RUT_DEUDOR, fa_docs + pa_docs, fa_pagos + pa_pagos, [])

    promedio = np.mean(PLAZOS_FA)            # 46
    desviacion = np.std(PLAZOS_FA)           # 4.0
    ultimos_5 = sorted(PLAZOS_FA, reverse=True)[:5]  # pagos más recientes = plazos más altos aquí
    esperado = max(30, round(np.mean(ultimos_5) + 0.5 * desviacion))

    assert r["cantidad_historico"] == len(PLAZOS_FA)
    assert r["promedio_historico"] == pytest.approx(promedio)
    assert r["desviacion_estandar"] == pytest.approx(desviacion)
    assert [p["plazo"] for p in r["ultimos_pagos"]] == [52, 50, 48, 46, 44]
    assert r["factura_mas_lenta"]["plazo"] == 52
    assert r["plazo_recomendado"] == esperado == 50
    assert all(p["clave_original"]["factura_doc"] < 900 for p in r["ultimos_pagos"])


def test_pagare_identificado_solo_por_tipo_de_pago_tambien_se_excluye(colecciones):
    # Doc marcado FA pero cuyo único pago viene como PAGARÉ: no genera plazo.
    fa_docs, fa_pagos = historial(RUT_DEUDOR, PLAZOS_FA)
    raro = doc(RUT_DEUDOR, 950, 501, tipo="FA", emision=EMISION_BASE)
    raro_pago = pago(RUT_DEUDOR, 950, 501, EMISION_BASE + timedelta(days=250), "PAGARÉ")

    r = api.procesar_rut(RUT_DEUDOR, fa_docs + [raro], fa_pagos + [raro_pago], [])
    # 250 días sería outlier y no cambiaría la cantidad, pero sí el promedio
    # histórico (que se calcula antes de quitar outliers): por eso se mira este.
    assert r["promedio_historico"] == pytest.approx(np.mean(PLAZOS_FA))
    assert r["desviacion_estandar"] == pytest.approx(np.std(PLAZOS_FA))


def test_historico_pagos_tampoco_muestra_pagares(colecciones):
    fa_docs, fa_pagos, pa_docs, pa_pagos = _mezclado()
    colecciones["docs"].documentos = fa_docs + pa_docs
    colecciones["pagos"].documentos = fa_pagos + pa_pagos

    r = api.historico_pagos(rut=RUT_DEUDOR)
    assert r["cantidad"] == len(PLAZOS_FA)
    assert sorted(p["plazo"] for p in r["pagos"]) == PLAZOS_FA


# ------------------------------------------------------------
# Deudor cuyo historial es solo de pagarés
# ------------------------------------------------------------

def test_solo_pagares_sin_empresa_registrada_devuelve_error_30(colecciones):
    pa_docs, pa_pagos = historial(RUT_SOLO_PAGARES, [60, 70, 80], tipo="PA")
    r = api.procesar_rut(RUT_SOLO_PAGARES, pa_docs, pa_pagos, [])

    assert r == {
        "error": "RUT no tiene historial ni está registrado en la base de empresas.",
        "plazo_recomendado": 30,
        "recomendacion": "No existe información histórica. Revisar empresas similares.",
    }


def test_solo_pagares_con_empresa_registrada_usa_similares(colecciones):
    pa_docs, pa_pagos = historial(RUT_SOLO_PAGARES, [60, 70, 80], tipo="PA")
    sim_fa_docs, sim_fa_pagos = historial(RUT_SIMILAR, [35, 45], tipo="FA", n_inicial=1, ope=600)
    sim_pa_docs, sim_pa_pagos = historial(RUT_SIMILAR, [200], tipo="PA", n_inicial=50, ope=601)

    colecciones["empresas"].documentos = [
        {"rut": RUT_SOLO_PAGARES, "nombre": "EMPRESA SOLO PAGARES", "rubro": "R1", "tramo_ventas": "T1"},
        {"rut": RUT_SIMILAR, "nombre": "EMPRESA SIMILAR", "rubro": "R1", "tramo_ventas": "T1"},
    ]
    # Los pagarés propios del deudor también están en la base, igual que en producción
    colecciones["docs"].documentos = pa_docs + sim_fa_docs + sim_pa_docs
    colecciones["pagos"].documentos = pa_pagos + sim_fa_pagos + sim_pa_pagos

    r = api.procesar_rut(RUT_SOLO_PAGARES, pa_docs, pa_pagos, [])

    assert r["empresas_similares"] is True
    assert r["cantidad_empresas_similares"] == 2          # solo las 2 facturas del similar
    assert r["promedio_empresas_similares"] == pytest.approx(40.0)
    assert r["desviacion_empresas_similares"] == pytest.approx(5.0)
    assert r["plazo_recomendado"] == max(30, round(40 + 0.5 * 5))  # 42


def test_solo_pagares_similares_sin_facturas_devuelve_30(colecciones):
    pa_docs, pa_pagos = historial(RUT_SOLO_PAGARES, [60], tipo="PA")
    sim_pa_docs, sim_pa_pagos = historial(RUT_SIMILAR, [90], tipo="PA", ope=601)
    colecciones["empresas"].documentos = [
        {"rut": RUT_SOLO_PAGARES, "nombre": "EMPRESA SOLO PAGARES", "rubro": "R1", "tramo_ventas": "T1"},
        {"rut": RUT_SIMILAR, "nombre": "EMPRESA SIMILAR", "rubro": "R1", "tramo_ventas": "T1"},
    ]
    colecciones["docs"].documentos = pa_docs + sim_pa_docs
    colecciones["pagos"].documentos = pa_pagos + sim_pa_pagos

    r = api.procesar_rut(RUT_SOLO_PAGARES, pa_docs, pa_pagos, [])
    assert r["error"] == "No se encontraron pagos de empresas similares."
    assert r["plazo_recomendado"] == 30


def test_solo_pagares_municipalidad_usa_regla_fija(colecciones):
    pa_docs, pa_pagos = historial(RUT_MUNI, [60, 70], tipo="PA")
    r = api.procesar_rut(RUT_MUNI, pa_docs, pa_pagos, [])
    assert r["tipo_entidad"] == "MUNICIPALIDAD"
    assert r["plazo_recomendado"] == 105


# ------------------------------------------------------------
# Morosos: la lista no cambia con la exclusión
# ------------------------------------------------------------

def test_lista_de_morosos_sigue_incluyendo_pagares(colecciones):
    fa_docs, fa_pagos = historial(RUT_DEUDOR, PLAZOS_FA)
    hoy = datetime.today()

    # Pagaré moroso sin pagos: aparecía en la lista y debe seguir apareciendo
    pa_moroso = doc(RUT_DEUDOR, 960, 700, tipo="PA", estado="MOROSO", emision=hoy - timedelta(days=120), monto=111)
    # Pagaré moroso con un abono PAGARÉ: antes no aparecía (tiene pago cruzado) y sigue sin aparecer
    pa_moroso_abono = doc(RUT_DEUDOR, 961, 700, tipo="PA", estado="MOROSO", emision=hoy - timedelta(days=90), monto=222)
    abono = pago(RUT_DEUDOR, 961, 700, hoy - timedelta(days=10), "PAGARÉ")
    # Factura morosa sin pagos: aparece
    fa_moroso = doc(RUT_DEUDOR, 962, 701, tipo="FA", estado="MOROSO", emision=hoy - timedelta(days=20), monto=333)

    facturas = fa_docs + [pa_moroso, pa_moroso_abono, fa_moroso]
    r = api.procesar_rut(RUT_DEUDOR, facturas, fa_pagos + [abono], morosos_de(facturas))

    assert sorted(m["monto"] for m in r["morosos"]) == [111, 333]
    # El abono del pagaré no alimenta los plazos
    assert r["cantidad_historico"] == len(PLAZOS_FA)
    # El pagaré moroso de 120 días supera el plazo recomendado (50): el riesgo se sigue detectando
    assert r["riesgo_detectado"] is True
