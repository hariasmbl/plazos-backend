"""
Tests de las reglas de verano (nov–feb) en procesar_rut.

La regla de verano solo puede alargar el plazo: se toma el mayor entre el
plazo de la regla y el del cálculo normal. Mismos fixtures sintéticos que
test_exclusion_pagares (sin conexión a la base real); el mes actual se
fija reemplazando datetime en los módulos.

Correr desde la raíz del repo:  python -m pytest tests -q
"""
from datetime import datetime

import pytest

from tests.test_exclusion_pagares import api, colecciones, historial  # noqa: F401
from scripts import consultor

RUT_MUNI = "69010100-9"
RUT_SERVIU = "61801000-7"
RUT_MOP = "61202000-0"


def fijar_mes(monkeypatch, mes, tambien_api=False):
    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, mes, 15)

    monkeypatch.setattr(consultor, "datetime", FakeDatetime)
    if tambien_api:
        # Solo sin historial: en consultor_api, parse_fecha usa isinstance(x, datetime)
        monkeypatch.setattr(api, "datetime", FakeDatetime)


@pytest.fixture
def verano(monkeypatch):
    fijar_mes(monkeypatch, 12)


@pytest.fixture
def fuera_de_verano(monkeypatch):
    fijar_mes(monkeypatch, 5)


def test_ruts_de_prueba_tienen_el_tipo_esperado():
    assert api.obtener_tipo_entidad(RUT_MUNI) == "MUNICIPALIDAD"
    assert api.obtener_tipo_entidad(RUT_SERVIU) == "SERVIU / MINVU"
    assert api.obtener_tipo_entidad(RUT_MOP) == "MOP"


# ------------------------------------------------------------
# Se toma el mayor entre regla y cálculo normal
# ------------------------------------------------------------

def test_muni_regla_menor_que_calculo_normal_usa_el_normal(colecciones, verano):
    # Historial antiguo rápido (base ≤ 45 → regla 45) y últimos 5 pagos lentos.
    # Pocos registros para que ningún pago caiga en meses de verano.
    plazos = [20] * 10 + [60] * 5
    d, p = historial(RUT_MUNI, plazos)
    r = api.procesar_rut(RUT_MUNI, d, p, [])

    normal = max(30, round(60 + 0.5 * r["desviacion_estandar"]))
    assert normal > 45
    assert r["plazo_recomendado"] == normal
    assert r["factor_dias"] == 7.5


def test_muni_regla_mayor_que_calculo_normal_usa_la_regla(colecciones, verano):
    d, p = historial(RUT_MUNI, [50, 52, 54, 56, 58])  # base 46–70 → 90
    r = api.procesar_rut(RUT_MUNI, d, p, [])
    assert r["plazo_recomendado"] == 90


def test_mop_regla_menor_que_calculo_normal_usa_el_normal(colecciones, verano):
    d, p = historial(RUT_MOP, [80, 82, 84, 86, 88])
    r = api.procesar_rut(RUT_MOP, d, p, [])
    assert r["plazo_recomendado"] > 60
    assert r["plazo_recomendado"] == max(30, round(84 + 0.5 * r["desviacion_estandar"]))


# ------------------------------------------------------------
# SERVIU / MINVU
# ------------------------------------------------------------

def test_serviu_verano_regla_90_si_el_normal_es_menor(colecciones, verano):
    d, p = historial(RUT_SERVIU, [40, 42, 44, 46, 48])
    r = api.procesar_rut(RUT_SERVIU, d, p, [])
    assert r["tipo_entidad"] == "SERVIU / MINVU"
    assert r["plazo_recomendado"] == 90
    assert r["factor_dias"] == 7.5


def test_serviu_verano_calculo_normal_si_es_mayor_que_90(colecciones, verano):
    d, p = historial(RUT_SERVIU, [110, 112, 114, 116, 118])
    r = api.procesar_rut(RUT_SERVIU, d, p, [])
    assert r["plazo_recomendado"] == max(30, round(114 + 0.5 * r["desviacion_estandar"]))
    assert r["plazo_recomendado"] > 90


def test_serviu_fuera_de_verano_usa_calculo_normal(colecciones, fuera_de_verano):
    d, p = historial(RUT_SERVIU, [40, 42, 44, 46, 48])
    r = api.procesar_rut(RUT_SERVIU, d, p, [])
    assert r["plazo_recomendado"] == max(30, round(44 + 0.5 * r["desviacion_estandar"]))
    assert r["factor_dias"] == 7.5


@pytest.mark.parametrize("mes,esperado", [(1, 90), (11, 90), (6, 180)])
def test_serviu_sin_historial(colecciones, monkeypatch, mes, esperado):
    fijar_mes(monkeypatch, mes, tambien_api=True)
    r = api.procesar_rut(RUT_SERVIU, [], [], [])
    assert r["plazo_recomendado"] == esperado
    assert f"{esperado} días" in r["recomendacion"]


def test_normal_fuera_de_verano_no_cambia(colecciones, fuera_de_verano):
    d, p = historial(RUT_MUNI, [20, 22, 24, 26, 28])
    r = api.procesar_rut(RUT_MUNI, d, p, [])
    assert r["plazo_recomendado"] == 30
    assert r["factor_dias"] == 15
