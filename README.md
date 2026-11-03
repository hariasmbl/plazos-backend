# Plazos — Consultor de plazos de pago (BL Capital)

Herramienta interna de BL Capital que recomienda cuántos días cubrir entre **plazo y anticipo** en una operación de factoring, según el historial real de pagos del **RUT deudor**.

- **Producción (frontend):** https://plazos-bl.web.app
- **Producción (backend):** https://plazos-backend.onrender.com
- **Repositorio:** https://github.com/hariasmbl/plazos-backend

---

## 1. Arquitectura

```
Navegador (login Google @blcapital.cl)
   │
   ▼
frontend/  ── HTML estático + Tailwind (CDN) + Firebase Auth
   │          alojado en Firebase Hosting (proyecto "plazos-bl")
   │  fetch()
   ▼
scripts/consultor_api.py ── FastAPI, alojado en Render (plan free)
   │
   ▼
MongoDB Atlas ── base "mi_base_datos"
```

| Colección   | Contenido                                                   | Origen                               |
|-------------|-------------------------------------------------------------|--------------------------------------|
| `docs`      | Facturas / documentos cedidos ("list docs")                 | Excel subido desde el Panel de carga |
| `pagos`     | Pagos recibidos (cartola)                                    | Excel subido desde el Panel de carga |
| `empresas`  | Base de empresas del SII (RUT, razón social, rubro, tramo)  | TXT del SII (~1,3 GB)                |
| `metadata`  | Estado de la última carga de cada tipo de archivo           | Lo escribe el backend                |

## 2. Estructura del repositorio

```
frontend/
  index.html        Página principal: consulta individual, consulta múltiple, simulador
  historico.html    Tabla completa de pagos históricos de un RUT
  admin.html        Panel de carga de archivos (solo admins)
  admins.txt        Correos con acceso al Panel de carga
  assets/logo.png
scripts/
  consultor_api.py  ★ Backend real (FastAPI). Aquí vive la lógica que usa producción
  consultor.py      Reglas de negocio compartidas (tipo de entidad, reglas de verano,
                    normalización de claves). OJO: su función consultar_por_rut() es
                    antigua y NO coincide con la de la API; no usarla como referencia
  cargar_datos.py   Carga de documentos (la usa el endpoint /subir-docs)
  cargar_pagos.py   Carga de pagos (la usa /subir-pagos)
  cargar_empresas.py Carga de la base SII en chunks (la usa /subir-empresas)
  data/listas_entidades/  Listas de RUT de municipalidades, corporaciones y SERVIU/MINVU
  borrar_*.py, duplicados.py, eliminar_duplicados.py, ver_*.py, test_conexion.py
                    Utilidades manuales de mantenimiento/debug (correr en local)
tests/
  test_exclusion_pagares.py  Tests con datos sintéticos (no tocan la base real)
firebase.json, .firebaserc   Configuración de Firebase Hosting
render.yaml                  Configuración de Render
requirements.txt             Dependencias Python
ruts_municipales_detectados.csv  Dato de apoyo (no lo usa la API)
```

## 3. Lógica de negocio (resumen)

Implementada en `procesar_rut()` / `_cruzar()` de `scripts/consultor_api.py`:

1. Se cruzan facturas (`docs`) con pagos (`pagos`) por la clave **(Nº documento, Nº operación)**, normalizada con `normalizar_clave()` (tolera ceros a la izquierda, `"123.0"`, etc.).
2. Plazo de cada factura = fecha de pago − fecha de emisión. Se descartan plazos anómalos (< 0 o > 300 días) y outliers estadísticos.
3. Los **pagarés** se excluyen del cálculo (rama `excluir-pagares-plazos`, ver §8).
4. Plazo recomendado = `max(30, promedio de los últimos 5 pagos + 0,5 × desviación estándar)`.
5. Reglas especiales según tipo de deudor (`obtener_tipo_entidad()` y `aplicar_reglas_verano()` en `consultor.py`; el tipo se detecta con las listas de `scripts/data/listas_entidades/`):
   - Solo en **temporada de verano (nov–feb)**, según el mes actual (fuera de verano se usa el cálculo normal):
     - **MOP:** 60 días.
     - **SERVIU / MINVU:** 90 días.
     - **Municipalidad / corporación municipal:** con base = promedio + 0,5 × desviación (de pagos de verano si los hay): ≤ 45 → 45 días; 46–70 → 90 días; 71–90 → 105 días; > 90 → se evalúa caso a caso (se mantiene el cálculo normal).
     - La regla de verano nunca acorta el plazo: si el cálculo normal (punto 4) da un plazo mayor, se usa ese.
   - `factor_dias` (días cubiertos por cada punto de anticipo) es 15 por defecto y 7,5 para entidades públicas en verano y para SERVIU/MINVU todo el año.
6. Si el RUT **no tiene historial**:
   - Entidad pública conocida → plazo fijo (municipalidad/corporación: 105 días; SERVIU/MINVU: 90 en verano, 180 el resto del año).
   - Empresa en la base SII → se usa el promedio de empresas del mismo **rubro y tramo de ventas**.
   - Si no hay nada → 30 días por defecto.
7. Se listan los documentos morosos (sin pago) y se marca riesgo si alguno lleva más días que el plazo recomendado.

En el frontend, el simulador (`simular()` en `index.html`) exige además que el plazo ingresado esté entre 30 y 100 días; fuera de ese rango la operación queda pendiente de revisión por Riesgo.

## 4. Endpoints del backend

| Método | Ruta                    | Uso                                                       |
|--------|-------------------------|-----------------------------------------------------------|
| GET    | `/`                     | Health check                                              |
| GET    | `/consultar-rut?rut=`   | Consulta individual                                       |
| POST   | `/consultar-ruts-batch` | Consulta múltiple (lista de RUTs)                         |
| POST   | `/extraer-ruts`         | Extrae RUTs desde un Excel/CSV subido (consulta múltiple) |
| GET    | `/historico-pagos?rut=` | Historial completo de pagos (historico.html)              |
| POST   | `/subir-docs`           | Sube Excel de documentos (procesa en background)          |
| POST   | `/subir-pagos`          | Sube Excel de pagos (procesa en background)               |
| POST   | `/subir-empresas`       | Sube TXT del SII (procesa en background)                  |
| GET    | `/estado-carga`         | Estado de las cargas (admin.html lo consulta cada 5 s)    |
| GET    | `/debug-format`, `/test-cruce`, `/test-pagos-keys` | Endpoints de diagnóstico       |

Documentación interactiva automática de FastAPI: `/docs`.

**Por qué las subidas son en background:** procesar un archivo de pagos grande de forma síncrona bloqueó el único worker de Render por ~10 minutos y dejó la API caída para todos. Por eso la subida responde de inmediato y el procesamiento corre aparte, dejando el estado en `metadata`.

## 5. Correr en local

Requisitos: Python 3.11+.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate    |    Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt
```

Crear un archivo `.env` en la raíz del repo (no se versiona):

```
MONGO_URI=mongodb+srv://<usuario>:<clave>@<cluster>/...
```

Levantar el backend **desde la raíz del repo** (el módulo importa `scripts.consultor`):

```bash
uvicorn scripts.consultor_api:app --reload --port 8000
```

Tests:

```bash
pip install pytest
python -m pytest tests -q
```

Frontend: son archivos estáticos. Se puede servir con `npx firebase serve` o cualquier servidor estático (p. ej. `python -m http.server 5000` dentro de `frontend/`). **Ojo:** la URL del backend está escrita directamente en los HTML (`https://plazos-backend.onrender.com`); para probar contra el backend local hay que cambiarla temporalmente. Los orígenes permitidos por CORS están en `consultor_api.py` (incluye `localhost:5000` y `localhost:5173`).

## 6. Despliegue

Son **dos pasos independientes**; ninguno dispara al otro:

- **Backend (Render):** un `git push` a `main` despliega automáticamente (`autoDeploy: true`). La variable `MONGO_URI` está configurada en el panel de Render. Revisar en el panel el *start command* efectivo: `render.yaml` dice `uvicorn consultor_api:app`, pero el código requiere ejecutarse como `scripts.consultor_api:app` desde la raíz.
- **Frontend (Firebase Hosting):**
  ```bash
  npx firebase login          # si da 401 aunque diga logueado: npx firebase login --reauth
  npx firebase deploy --only hosting
  ```

## 7. Accesos y seguridad

- Login con Google vía Firebase Auth, restringido a correos `@blcapital.cl`.
- El Panel de carga solo se muestra a los correos de `frontend/admins.txt`.
- **Importante:** ambas validaciones se hacen solo en el navegador. El backend no verifica tokens, por lo que cualquiera que conozca la URL de la API puede llamarla. Es una mejora pendiente si el proyecto crece.
- La `apiKey` de Firebase que aparece en los HTML es pública por diseño (no es un secreto).
- El único secreto es `MONGO_URI`. **No está en el repositorio** y se entrega por un canal separado.

## 8. Estado actual y pendientes conocidos

- La rama `excluir-pagares-plazos` (exclusión de pagarés del cálculo) tiene un commit que **aún no está en `main` ni en producción**.
- Render y MongoDB Atlas están en plan gratuito: tras varios días sin uso, la app puede tardar desde segundos hasta ~30 minutos en "despertar". Antes de buscar un bug, reintentar pasados unos minutos.
- Varias utilidades de `scripts/` tienen rutas de Windows escritas en el código y están pensadas solo para uso local.
- `consultor.py → consultar_por_rut()` es una versión antigua de la lógica; la fuente de verdad es `consultor_api.py`.
- Las carpetas `data/` y `Otros_docs/` (Excels de origen) no se versionan porque contienen datos de clientes.
